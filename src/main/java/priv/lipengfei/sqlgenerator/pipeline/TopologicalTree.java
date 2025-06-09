package priv.lipengfei.sqlgenerator.pipeline;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import priv.lipengfei.sqlgenerator.cells.Cell;
import priv.lipengfei.sqlgenerator.cells.Edge;
import priv.lipengfei.utils.JSONParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author lipengfei
 */
@Slf4j
@Data
public class TopologicalTree<T extends Cell>{
    private Map<String, TopologicalTreeNode> items = new HashMap<>();


    public List<String> getMergeCells() {
        return items.values().stream().filter(c -> c.getNode() instanceof MergeItem).map(c -> c.getNode().getId()).toList();
    }

    public List<String> getInputs(){
        return items.values().stream().filter(c -> !(c.getNode() instanceof MergeItem) && c.getOutDegree()>0 && c.getInDegree() == 0).map(c -> c.getNode().getId()).toList();
    }

    public List<String> getOutputs(){
        return items.values().stream().filter(c -> c.getOutDegree() == 0 && c.getInDegree()>0).map(c -> c.getNode().getId()).toList();
    }

    public List<Pipeline> getPipelines() {
        Map<Pipeline, Boolean> pipelines = new HashMap<>();
        List<String> inputs = getInputs();
        // 初始化pipeline列表
        for (String inputId : inputs) {
            Pipeline pipeline = new Pipeline(items.get(inputId).getNode());
            pipelines.put(pipeline, false);
        }

        log.info("===========================================");
        log.info(String.join(",", inputs));
        List<String> mergeCellOutputs = getMergeCells().stream().flatMap(p -> items.get(p).getNextNodes().stream()).distinct().toList();
        for (String mergeCell : mergeCellOutputs) {
            Pipeline pipeline = new Pipeline(items.get(mergeCell).getNode());
            pipelines.put(pipeline, false);
        }

        log.info("===========================================");
        log.info(String.join(",", mergeCellOutputs));

        log.info("===========================================");
        pipelines.forEach((k, p) -> {
            log.info("{} --> {}", k, p);
        });
        // 获得子节点，直到没有
        while(pipelines.containsValue(false)){
            for (Pipeline pipeline : pipelines.keySet().stream().toList()) {
                if(!pipelines.get(pipeline)) {
                    List<String> nextNodes = items.get(pipeline.getLastNode().getId()).getNextNodes();
                    nextNodes.forEach(n -> {
                        TopologicalTreeNode treeNode = items.get(n);

                        Pipeline newPipeline = new Pipeline(pipeline);
                        if (treeNode.getNode() instanceof MergeItem){
                            pipelines.put(newPipeline, true);
                        }else if(treeNode.getOutDegree() == 0) {
                            newPipeline.addNodes(treeNode.getNode());
                            pipelines.put(newPipeline, true);
                        } else {
                            newPipeline.addNodes(treeNode.getNode());
                            pipelines.put(newPipeline, false);
                        }
                    });
                    pipelines.remove(pipeline);
                }
            }
            log.info("===========================================");
            pipelines.forEach((k, p) -> {
                log.info("{} --> {}", k, p);

            });
        }

        log.info("===========================================");
        pipelines.forEach((k, p) -> log.info(String.valueOf(k)));
        return new ArrayList<>(pipelines.keySet());
    }
    /**
     * 添加节点
     * @param node
     * @return
     */
    public TopologicalTree addNode(Cell node) {
        // 如果节点已经存在, 则不添加
        if(items.containsKey(node.getId())) {
            log.error("{} 节点已经存在", node.getId());
            return this;
        }
        if(node instanceof Edge) {
            log.error("Edge can't be added to pipeline");
        }else {
            TopologicalTreeNode item = new TopologicalTreeNode()
                    .setNode(node);
            items.putIfAbsent(node.getId(), item);
        }
        return this;
    }

    /**
     * 删除节点, 同时删除所有与该节点相关的边
     * @param id
     * @return
     */
    public TopologicalTree removeNode(String id) {
        if(!items.containsKey(id)) {
            // 如果没有该节点id
            log.error("{} 节点不存在", id);
        }else {
            items.forEach((k, v) -> {
                v.getParentNodes().remove(id);
                v.getNextNodes().remove(id);
            });
            items.remove(id);

        }
        return this;
    }

    public TopologicalTree addEdge(Edge e) {
        if (!items.containsKey(e.getSourceId()) || !items.containsKey(e.getTargetId())) {
            throw new IllegalArgumentException("节点不存在");
        }
        // 判断是否存在这条边
        if (items.get(e.getSourceId()).getNextNodes().contains(e.getTargetId())) {
            log.error("{}->{}, 这条边已经存在!!!", e.getSourceId(), e.getTargetId());
            return this;
        }
        items.get(e.getSourceId())
                .addNextNode(e.getTargetId());

        items.get(e.getTargetId()).addParentNode(e.getSourceId());

        if (!checkAvailable()) {
            log.error("Pipeline is not available");
        }
        return this;
    }

    /**
     * Pipeline 插入cells
     * 如果为一进一出的item, 则直接插入items
     * 如果为边的edge, 则需要判断source和target是否在items中, 如果在, 则插入ids
     *
     * @param cells
     * @return
     */
    public TopologicalTree addCells(Cell... cells){
        if(items == null) {
            items = new HashMap<>();
        }
        Stream.of(cells).filter(cell -> !(cell instanceof Edge)).forEach(this::addNode);
        Stream.of(cells).filter(cell -> cell instanceof Edge).forEach(e -> this.addEdge((Edge) e));

        if (!checkAvailable()) {
            log.error("Pipeline is not available");
        }
        return this;
    }

    public boolean checkAvailable(){
        // TODO 不能有环
        // 除了MergeItem入度均小于2
        // 只能有一个入度为0且ch
        // 检查pipeline是否有效
        if(items.values().stream().anyMatch(item -> item.getInDegree() > 1 || item.getOutDegree() > 1)){
            // 有入度和出度大于1的，不是pipeline
            log.error("有入度和出度大于1的，不是pipeline");
            return false;
        }else if(items.values().stream().filter(item -> item.getOutDegree()>0 && item.getInDegree() == 0).count() > 1){
            // 入度为0的节点大于1个，不是pipeline
            log.error("入度为0的节点大于1个，不是pipeline");
            return false;
        }else if(items.values().stream().filter(item -> item.getOutDegree() == 0 && item.getInDegree() > 0).count() <= 1){
            log.error("Pipeline is Available!!!");
            return true;
        }else{
            // 出度为0的节点大于1个，不是pipeline
            log.error("出度为0的节点大于1个，不是pipeline");
            return false;
        }
    }

    public List<Cell> getCells(){
        // 获得pipeline节点
        return items.values().stream().map(TopologicalTreeNode::getNode).toList();
    }

    public List<Edge> getEdges(){
        return this.items.values().stream().flatMap(c -> c.getNextNodes().stream().map(n -> new Edge(c.getNode().getId(), n))).toList();
    }

    public TopologicalTree mergePipeline(TopologicalTree<T> pipe){
        List<Edge> edges = pipe.getEdges();
        List<Cell> cells = pipe.getCells();

        cells.forEach(this::addCells);
        edges.forEach(this::addEdge);
        return this;
    }

    public boolean hasCell(String id){
        return this.items.containsKey(id);
    }

    public Cell getCell(String id){
        return this.items.get(id).getNode();
    }
//
//    public String toJson(){
////        StringBuilder s = new StringBuilder();
//        List<Map<String, Object>> maps = new ArrayList<>();
//        this.items.forEach((k, v) -> {
//            Map<String, Object> stringObjectMap = CellsJsonParser.toMapByReflect(v);
//            stringObjectMap.put("class", v.getClass().getName());
//            maps.add(stringObjectMap);
//        });
//
//        return JSONParser.toJsonString(maps);
//    }

    public static Pipeline fromJson(String s){
        List map = JSONParser.fromJsonString(s, List.class);
        System.out.println(map);
        return new Pipeline();
    }

    @Override
    public String toString() {
        String nodes = String.join(" \n ", this.getCells().stream().map(Cell::getId).toList());
        String edges = String.join(" \n ", this.getEdges().stream().map(Edge::toString).toList());
        return String.format("拓扑树：\n{ \n nodes: \n %s \n edges: \n %s \n}", nodes, edges);
    }
}
