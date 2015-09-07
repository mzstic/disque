<?php
namespace Disque\Connection;

use Disque\Connection\Node\Node;
use Disque\Command\CommandInterface;
use Disque\Command\GetJob;
use Disque\Command\Response\HelloResponse;
use \Disque\Command\Response\JobsResponse;
use Disque\Connection\Factory\ConnectionFactoryInterface;
use Disque\Connection\Factory\SocketFactory;

/**
 * The connection manager connects to Disque nodes and chooses the best of them
 *
 * If there are multiple nodes to connect, the first connection is always
 * random. The manager then switches to the best nodes according to its
 * NodePriority strategy.
 *
 * If the manager knows the credentials of only one node, it will automatically
 * discover other nodes in the cluster and connect to them if needed (unless
 * they are password-protected).
 */
class Manager implements ManagerInterface
{
    /**
     * Servers we can connect to initially, without knowing the cluster
     *
     * After connecting to one, the server returns a list of other nodes
     * in the cluster so we can connect to them automatically, unless
     * the discovered nodes are secured with a password.
     *
     * 'serverAddress' => Credentials
     *
     * @var Credentials[]
     */
    protected $credentials = [];

    /**
     * If a node has produced at least these number of jobs, switch there
     *
     * @var int
     */
    protected $minimumJobsToChangeNode = 0;

    /**
     * List of nodes, ie Disque instances available in the cluster
     *
     * 'nodeId' => Node
     *
     * @var Node[]
     */
    protected $nodes = [];

    /**
     * Node prefixes and their corresponding node ID
     *
     * Node prefix consists of the first 8 bytes from the node ID. Because job
     * IDs contain the node prefix, it can be used to identify on which node
     * a job lives.
     *
     * 'nodePrefix' => 'nodeId'
     *
     * @var array
     */
    protected $nodePrefixes = [];

    /**
     * The ID of the node we are currently connected to
     *
     * @var string
     */
    protected $nodeId;

    /**
     * @var ConnectionFactoryInterface
     */
    private $connectionFactory;

    public function __construct()
    {
        $this->connectionFactory = new SocketFactory();
    }

    /**
     * @inheritdoc
     */
    public function getConnectionFactory()
    {
        return $this->connectionFactory;
    }

    /**
     * @inheritdoc
     */
    public function setConnectionFactory(
        ConnectionFactoryInterface $connectionFactory
    ) {
        $this->connectionFactory = $connectionFactory;
    }

    /**
     * @inheritdoc
     */
    public function getCredentials()
    {
        return $this->credentials;
    }

    /**
     * @inheritdoc
     */
    public function addServer(Credentials $credentials)
    {
        $address = $credentials->getAddress();
        $this->credentials[$address] = $credentials;
    }

    /**
     * @inheritdoc
     */
    public function setMinimumJobsToChangeNode($minimumJobsToChangeNode)
    {
        $this->minimumJobsToChangeNode = $minimumJobsToChangeNode;
    }

    /**
     * @inheritdoc
     */
    public function isConnected()
    {
        return (
            isset($this->nodeId) &&
            $this->nodes[$this->nodeId]->getConnection()->isConnected()
        );
    }

    /**
     * @inheritdoc
     */
    public function connect()
    {
        $currentNode = $this->findAvailableConnection();
        $this->switchToNode($currentNode);
        return $currentNode;
    }

    /**
     * @inheritdoc
     */
    public function execute(CommandInterface $command)
    {
        $this->shouldBeConnected();
        $response = $this->nodes[$this->nodeId]->getConnection()->execute($command);
        if ($command instanceof GetJob) {
            $this->updateNodeStats($command->parse($response));
            $this->switchNodeIfNeeded();
        }
        return $response;
    }

    public function getNodes()
    {
        return $this->nodes;
    }

    /**
     * Get a functional connection to any known node
     *
     * Disque suggests the first connection should be chosen randomly
     *
     * @return Node A connected node
     *
     * @throws AuthenticationException
     * @throws ConnectionException
     */
    protected function findAvailableConnection()
    {
        $servers = $this->credentials;
        while (!empty($servers)) {
            $key = array_rand($servers);
            $server = $servers[$key];
            $node = $this->getNodeConnection($server);
            if ($node->getConnection()->isConnected()) {
                return $node;
            }
            unset($servers[$key]);
        }

        throw new ConnectionException('No servers available');
    }

    /**
     * Connect to the node given in the credentials
     *
     * @param Credentials $server
     *
     * @return Node A connected node
     */
    protected function getNodeConnection(Credentials $server)
    {
        $node = $this->createNode($server);
        $node->connect();
        return $node;
    }

    /**
     * Update node counters indicating how many jobs the node has produced
     *
     * @param array $jobs Jobs
     */
    private function updateNodeStats(array $jobs)
    {
        foreach ($jobs as $job) {
            $nodeId = $this->getNodeIdFromJobId($job[JobsResponse::KEY_ID]);
            if (!isset($nodeId) or !isset($this->nodes[$nodeId])) {
                continue;
            }

            $node = $this->nodes[$nodeId];
            $node->addJobCount(1);
        }
    }

    /**
     * Decide if we should switch to a better node
     */
    private function switchNodeIfNeeded()
    {
        // TODO: Implement the node prioritizer
        $sortedNodes = $this->nodePrioritizer->sort(
            $this->nodes,
            $this->nodeId
        );

        foreach($sortedNodes as $nodeCandidate) {
            if ($nodeCandidate->getId() === $this->nodeId) {
                return;
            }

            if ($nodeCandidate->getConnection()->isConnected() === false) {
                try {
                    $nodeCandidate->connect();
                } catch (ConnectionException $e) {
                    continue;
                }
            }

            $this->switchToNode($nodeCandidate);
        }
    }

    /**
     * Get node ID based off a Job ID
     *
     * @param string       $jobId Job ID
     * @return string|null        Node ID
     */
    private function getNodeIdFromJobId($jobId)
    {
        $nodePrefix = $this->getNodePrefixFromJobId($jobId);
        if (
            isset($this->nodePrefixes[$nodePrefix]) and
            array_key_exists($this->nodePrefixes[$nodePrefix], $this->nodes)
        ) {
            return $this->nodePrefixes[$nodePrefix];
        }

        return null;
    }

    /**
     * Get the node prefix from the job ID
     *
     * @param string  $jobId
     * @return string        Node prefix
     */
    private function getNodePrefixFromJobId($jobId)
    {
        $nodePrefix = substr(
            $jobId,
            JobsResponse::ID_NODE_PREFIX_START,
            Node::PREFIX_LENGTH
        );

        return $nodePrefix;
    }

    /**
     * We should be connected
     *
     * @return void
     * @throws ConnectionException
     */
    private function shouldBeConnected()
    {
        if (!$this->isConnected()) {
            throw new ConnectionException('Not connected');
        }
    }

    /**
     * Create a new Node object
     *
     * @param Credentials $credentials
     *
     * @return Node An unconnected Node
     */
    private function createNode(Credentials $credentials)
    {
        $host = $credentials->getHost();
        $port = $credentials->getPort();
        $connection = $this->connectionFactory->create($host, $port);

        return new Node($credentials, $connection);
    }

    /**
     * Switch to the given node and map the cluster from its HELLO
     *
     * @param Node $node
     */
    private function switchToNode(Node $node)
    {
        if ($this->nodeId === $node->getId()) {
            return;
        }

        $this->nodeId = $node->getId();

        $this->nodes = [];
        $this->nodes[$this->nodeId] = $node;

        $hello = $node->getHello();
        $this->revealClusterFromHello($hello);
    }

    /**
     * Reveal the whole Disque cluster from a node HELLO response
     *
     * The HELLO response from a Disque node contains addresses of all other
     * nodes in the cluster. We want to learn about them and save them, so that
     * we can switch to them later, if needed.
     *
     * @param array $hello
     */
    private function revealClusterFromHello(array $hello)
    {
        foreach ($hello[HelloResponse::NODES] as $node) {
            $id = $node[HelloResponse::NODE_ID];

            $prefix = substr($id, Node::PREFIX_START, Node::PREFIX_LENGTH);
            $this->nodePrefixes[$prefix] = $id;

            if ($this->nodeId === $id) {
                // The current node has already been added, don't overwrite it
                continue;
            }

            $host = $node[HelloResponse::NODE_HOST];
            $port = $node[HelloResponse::NODE_PORT];
            $credentials = new Credentials($host, $port);

            $address = $credentials->getAddress();
            // Check if the node credentials have already been set by the user
            // The user-supplied credentials may contain a password
            if (isset($this->credentials[$address])) {
                $credentials = $this->credentials[$address];
            }

            $this->nodes[$id] = $this->createNode($credentials);
        }
    }
}
