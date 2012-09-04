package tajo.engine.planner.physical;

import tajo.SubqueryContext;
import tajo.catalog.Schema;
import tajo.engine.exec.eval.EvalContext;
import tajo.engine.exec.eval.EvalNode;
import tajo.engine.planner.logical.SelectionNode;
import tajo.storage.RowStoreUtil;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import java.io.IOException;

/**
 * @author : hyunsik
 */
public class SelectionExec extends PhysicalExec  {
  private final SelectionNode annotation;
  private final PhysicalExec subOp;
  private final Schema inSchema;
  private final Schema outSchema;

  private final EvalNode qual;
  private final EvalContext qualCtx;
  private final Tuple outputTuple;
  // projection
  private int [] targetIds;

  public SelectionExec(SubqueryContext ctx, SelectionNode selNode,
                       PhysicalExec subOp) {
    this.annotation = selNode;
    this.inSchema = selNode.getInSchema();
    this.outSchema = selNode.getOutSchema();
    this.subOp = subOp;

    this.qual = this.annotation.getQual();
    this.qualCtx = this.qual.newContext();
    // for projection
    if (!inSchema.equals(outSchema)) {
      targetIds = RowStoreUtil.getTargetIds(inSchema, outSchema);
    }

    this.outputTuple = new VTuple(outSchema.getColumnNum());
  }

  @Override
  public Schema getSchema() {
    return annotation.getOutSchema();
  }

  @Override
  public Tuple next() throws IOException {
    Tuple tuple;
    while ((tuple = subOp.next()) != null) {
      qual.eval(qualCtx, inSchema, tuple);
      if (qual.terminate(qualCtx).asBool()) {
        if (targetIds != null) {
          RowStoreUtil.project(tuple, outputTuple, targetIds);
          return outputTuple;
        } else {
          return tuple;
        }
      }
    }

    return null;
  }

  @Override
  public void rescan() throws IOException {
    subOp.rescan();
  }
}