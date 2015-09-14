<?php
namespace Disque\Connection\Node;

use InvalidArgumentException;

/**
 * A prioritizer switching nodes if they have more jobs by a given margin
 * 
 * This class prioritizes nodes by job count. Because there is a cost to switch,
 * it doesn't switch from the current node unless the new candidate has a safe
 * margin over the current node.
 * 
 * This margin can be set manually and defaults to 5%, ie. the new candidate
 * must have 5% more jobs than the current node.
 *
 * This parameter makes the prioritizer behave conservatively - it prefers
 * the status quo and won't switch immediately if the difference is small.
 *
 * You can make the prioritizer eager by setting the margin to 0, or more
 * conservative by setting it higher. Setting the margin to negative values
 * is not allowed.
 */
class ConservativeJobCountPrioritizer implements NodePrioritizerInterface
{
    /**
     * @var float A margin to switch from the current node
     *
     * 0.05 means the new node must have 5% more jobs than the current node
     * in order to recommend switching over.
     */
    private $marginToSwitch = 0.05;
    
    /**
     * Get the margin to switch
     * 
     * @return float
     */
    public function getMarginToSwitch()
    {
        return $this->marginToSwitch;
    }
    
    /**
     * Set the margin to switch
     *
     * @param float $marginToSwitch A positive float or 0
     *
     * @throws InvalidArgumentException
     */
    public function setMarginToSwitch($marginToSwitch)
    {
        if ($marginToSwitch < 0) {
            throw new InvalidArgumentException('Margin to switch must not be negative');
        }

        $this->marginToSwitch = $marginToSwitch;
    }

    /**
     * @inheritdoc
     */
    public function sort(array $nodes, $currentNodeId)
    {
        // Optimize for a "cluster" consisting of just 1 node - skip everything
        if (count($nodes) === 1) {
            return $nodes;
        }

        uasort($nodes, function(Node $nodeA, Node $nodeB) use ($currentNodeId) {
            $priorityA = $this->getJobCountWithMargin($nodeA, $currentNodeId);
            $priorityB = $this->getJobCountWithMargin($nodeB, $currentNodeId);

            if ($priorityA === $priorityB) {
                return 0;
            }

            return ($priorityA < $priorityB) ? -1 : 1;
        });

        return $nodes;
    }

    /**
     * Calculate the node priority from its job count, stick to the current node
     *
     * @param Node   $node
     * @param string $currentNodeId
     *
     * @return float Node priority
     */
    private function calculateNodePriority(Node $node, $currentNodeId)
    {
        $priority = $node->getJobCount();

        if ($node->getId() === $currentNodeId) {
            $margin = 1 + $this->marginToSwitch;
            $priority = $priority * $margin;
        }

        return (float) $priority;
    }
    
    
}
