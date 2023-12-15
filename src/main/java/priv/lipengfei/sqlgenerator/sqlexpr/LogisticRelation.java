package priv.lipengfei.sqlgenerator.sqlexpr;

import lombok.AllArgsConstructor;
import lombok.Getter;
import priv.lipengfei.basic.TreeNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@AllArgsConstructor
@Getter
public class LogisticRelation extends TreeNode {
    private String role; // 角色分为root、and和or

    @Override
    public void setChildNodes(List<TreeNode> childNodes) {
        // List要指定ArrayList等形式，不能用this.childNodes = childNodes;，会用bug
        // TODO：修改其他相关问题
        this.childNodes = new ArrayList<>(childNodes);
    }

    /**
     * 检查该树是否最简
     * 树的每一层角色都是都是一样的，
     * @return boolean true是最简，false不是最简
     */
    public boolean checkTreeNode(){
        // 检查自己的儿子层和自己的角色有没有一致的，有是true，没有是false
//        boolean reduce = this.childNodes.parallelStream().filter(item -> item.getClass() == LogisticRelation.class)
//                .map(item -> Objects.equals(this.role, ((LogisticRelation) item).role))
//                .reduce((a, b) -> a || b);
        // 有一致的就不是最简的，返回false
        if(this.getProbSons().size()>0)
            return false;

        // 检查自己的儿子是不是最简树, true就是最简树，false就是不是最简树
        Optional<Boolean> reduce1 = this.childNodes.parallelStream()
                .filter(item -> item.getClass() == LogisticRelation.class)
                .map(item -> ((LogisticRelation) item).checkTreeNode())
                .reduce((a, b) -> a && b);

        return reduce1.orElse(true);
    }

    // 自己的儿子层和自己的角色有没有一致的个数
    public List<LogisticRelation> getProbSons(){
        return this.childNodes.parallelStream()
                .filter(item -> item.getClass() == LogisticRelation.class
                        && Objects.equals(((LogisticRelation) item).role, this.role))
                .map(item -> (LogisticRelation)item).toList();
    }
    /**
     * 把树变成最简树
     */
    public void fitTree(){
        // 自己的儿子都是whereCondition就停止
        // 计算自己儿子有多少个LogisticRelation并和自己不一致
        List<LogisticRelation> sons = this.getProbSons();

        System.out.println(sons.size());
        // 将自己和自己的儿子层弄成不一致
        while(sons.size()>0) {
            // 将和自己一样的儿子合并
            sons.parallelStream()
                    .filter(item -> Objects.equals(item.role, this.role))
                    .forEach(item -> {
                        this.childNodes.remove(item);
                        this.extendChild(item.childNodes);
                    });
            // 重新获得一致角色的儿子，直到全都不一致
            sons = this.getProbSons();
        }
        // 同样去弄自己的儿子们
        this.childNodes.parallelStream()
                .filter(item -> item.getClass() == LogisticRelation.class)
                .forEach(item -> ((LogisticRelation) item).fitTree());
    }

    private void extendChild(List<TreeNode> childNodes) {
        this.childNodes.addAll(childNodes);
    }
}
