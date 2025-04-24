package oafp.model;

import java.util.*;

/**
 * 表示整个拓扑图，包含所有任务节点及边关系
 */
public class StreamTopology {
    // 存储所有节点的映射，键为节点id，值为OperatorNode对象
    private final Map<String, OperatorNode> nodes = new HashMap<>();

    public void addOperator(OperatorNode node) {
        nodes.put(node.id, node);
    }

    // 根据节点id获取节点
    public OperatorNode get(String id) {
        return nodes.get(id);
    }

    // 获取所有节点
    public Collection<OperatorNode> getAllOperators() {
        return nodes.values();
    }

    /**
     * 查找从所有节点到指定sink节点的所有路径
     * @param sink 目标sink节点
     * @return 返回一个包含所有路径的列表，每个路径是一个操作节点列表
     */
    public List<List<OperatorNode>> findAllPathsToSink(OperatorNode sink) {
        // 存储所有路径的列表
        List<List<OperatorNode>> paths = new ArrayList<>();

        Deque<OperatorNode> stack = new ArrayDeque<>();
        // 调用深度优先搜索方法
        dfs(sink, stack, paths);

        return paths;
    }

    /**
     * 深度优先搜索方法，用于查找路径
     * @param current 当前节点
     * @param stack 搜索栈
     * @param paths 存储路径的列表
     */
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
