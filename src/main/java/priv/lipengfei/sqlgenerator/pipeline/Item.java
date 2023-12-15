package priv.lipengfei.sqlgenerator.pipeline;


import priv.lipengfei.utils.Utils;

// 一进一出
public abstract class Item {
    protected String id;
    protected String name; // item名字
    protected String className = this.getClass().getSimpleName();

    public Item(){
        this.id = String.valueOf(Utils.get64MostSignificantBitsForVersion1());
        this.name = String.format("%s%s", this.getClass().getSimpleName(), this.id.substring(this.id.length()-8));
    }

    public void setClassName(){
        this.className = this.getClass().getSimpleName();
    }
    // 模拟执行操作
    public abstract DataTable execute(DataTable table);
}
