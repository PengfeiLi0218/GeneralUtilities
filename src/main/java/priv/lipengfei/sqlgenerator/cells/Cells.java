package priv.lipengfei.sqlgenerator.cells;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import priv.lipengfei.sqlgenerator.pipeline.MergeItem;
import priv.lipengfei.sqlgenerator.pipeline.Pipeline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author lipengfei
 */
@Data
@Slf4j
public class Cells {
    private List<Cell> cells = new ArrayList<>();
    private Map<String, Cell> cellMap = new HashMap<>();
    private List<Edge> edges = new ArrayList<>();
    private List<MergeItem> merges = new ArrayList<>();
    private List<Pipeline> pipelines = new ArrayList<>();

    public Cells setCells(Cell... cells) {
        Stream.of(cells).forEach(c -> {
            if(c instanceof Edge e){
                this.edges.add((Edge) c);
                String targetId = e.getTargetId();
                String sourceId = e.getSourceId();

                List<Pipeline> targetPipelines = this.pipelines.stream().filter(pnode -> pnode.hasCell(targetId)).toList();
                List<Pipeline> sourcePipelines = this.pipelines.stream().filter(pnode -> pnode.hasCell(sourceId)).toList();


            }else {
                this.pipelines.add(new Pipeline().addNodes(c));
                this.cells.add(c);
                this.cellMap.put(c.id, c);
            }
        });
        return this;
    }

    public List<Edge> getEdges(){
        return this.edges;
    }

    public List<? extends Cell> getPrevious(String id){
        return this.getEdges().stream()
                .filter(edge -> edge.getTargetId().equals(id))
                .map(c -> this.cellMap.get(c.getSourceId()))
                .toList();
    }

    public List<MergeItem> getMerges(){
        return this.cells.stream().filter(cell ->  cell instanceof MergeItem).map(cell -> (MergeItem) cell).collect(Collectors.toList());
    }

    public boolean hasPrevious(String id){
        return getPrevious(id).stream().anyMatch(cell ->
            !(cell instanceof MergeItem)
        );
    }

    public List<? extends Cell> getNext(String id){
        List<String> nodeIds = this.getEdges().stream()
                .filter(edge -> edge.getSourceId().equals(id))
                .map(Edge::getTargetId)
                .toList();

        return this.getCells().stream().filter(c -> nodeIds.contains(c.getId())).toList();
    }

    public boolean hasNext(String id){
        return getNext(id).stream().anyMatch(c -> !(c instanceof MergeItem));
    }

    public Pipeline getPipeline(String id){
        // 通过头id获得整个pipeline
        if(hasPrevious(id)){
            log.error("该节点有前驱节点，无法获得pipeline");
            return null;
        }else if(hasNext(id)){
            List<? extends Cell> cells = this.getNext(id);
            new Pipeline();
            return null;
        }else{
            log.error("该节点没有后置节点");
            return null;
        }
    }

    public List<Pipeline> getAllPipelines(){
        // 1. 获得merge节点
        // 2. merge节点的输入为尾，merge节点的输出为头
        // 3. source table为头， sink table为尾
        Pipeline pipeline = new Pipeline();
        List<Edge> edges = this.getEdges();

        List<String> targetIds = edges.stream().map(Edge::getTargetId).toList();
        List<String> sourceIds = edges.stream().filter(edge -> !targetIds.contains(edge.getSourceId())).map(Edge::getSourceId).toList();


        return null;

    }
}
