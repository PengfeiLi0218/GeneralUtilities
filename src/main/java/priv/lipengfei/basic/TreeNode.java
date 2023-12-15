package priv.lipengfei.basic;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
public class TreeNode {
    protected List<TreeNode> childNodes = new ArrayList<>();

    protected String className = this.getClass().getSimpleName();

    public boolean isLeaf(){
        return this.childNodes.size()==0;
    }

    public void addChild(TreeNode childNode) {
        this.childNodes.add(childNode);
    }


}
