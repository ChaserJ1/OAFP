package oafp.model;

import java.util.*;

/**
 * 表示整个拓扑图，包含所有任务节点及边关系
 */
public class StreamTopology {
    private final Map<String, OperatorNode> nodes = new HashMap<>();

    public void addOperator(OperatorNode node) {
        nodes.put(node.id, node);
    }

    public OperatorNode get(String id) {
        return nodes.get(id);
    }

    public Collection<OperatorNode> getAllOperators() {
        return nodes.values();
    }

    public List<List<OperatorNode>> findAllPathsToSink(OperatorNode sink) {
        List<List<OperatorNode>> paths = new ArrayList<>();
        Deque<OperatorNode> stack = new ArrayDeque<>();
        dfs(sink, stack, paths);
        return paths;
    }

    private void dfs(OperatorNode current, Deque<OperatorNode> stack, List<List<OperatorNode>> paths) {
        stack.push(current);
        if (current.upstream.isEmpty()) {
            paths.add(new ArrayList<>(stack));
        } else {
            for (OperatorNode up : current.upstream) {
                dfs(up, stack, paths);
            }
        }
        stack.pop();
    }
}
