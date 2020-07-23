package is.hail.expr.ir.lowering

import is.hail.expr.Nat
import is.hail.expr.ir._
import is.hail.expr.ir.functions.GetElement
import is.hail.types.{BlockMatrixSparsity, BlockMatrixType, TypeWithRequiredness}
import is.hail.types.virtual._
import is.hail.utils.{FastIndexedSeq, FastSeq}

object BlockMatrixStage {
  def empty(eltType: Type): BlockMatrixStage =
    EmptyBlockMatrixStage(eltType)

  def broadcastVector(vector: IR, typ: BlockMatrixType, asRowVector: Boolean): BlockMatrixStage = {
    val v = Ref(genUID(), vector.typ)
    new BlockMatrixStage(Array(v.name -> vector), TStruct("start" -> TInt32, "shape" -> TTuple(TInt32, TInt32))) {
      def blockContext(idx: (Int, Int)): IR = {
        val (i, j) = typ.blockShape(idx._1, idx._2)
        val start = (if (asRowVector) idx._1 else idx._2) * typ.blockSize
        makestruct("start" -> start, "shape" -> MakeTuple.ordered(FastSeq[IR](i.toInt, j.toInt)))
      }

      def blockBody(ctxRef: Ref): IR = {
        bindIR(GetField(ctxRef, "shape")) { shape =>
          val len = if (asRowVector) GetTupleElement(shape, 1) else GetTupleElement(shape, 0)
          val nRep = if (asRowVector) GetTupleElement(shape, 0) else GetTupleElement(shape, 1)
          val start = GetField(ctxRef, "start")
          bindIR(NDArrayReshape(
            NDArraySlice(v, MakeTuple.ordered(FastSeq(MakeTuple.ordered(FastSeq(start.toL, (start + len).toL, 1L))))),
            MakeTuple.ordered(if (asRowVector) FastSeq[IR](1L, len.toL) else FastSeq[IR](len.toL, 1L)))) { sliced =>
            NDArrayConcat(ToArray(mapIR(rangeIR(nRep))(_ => sliced)), if (asRowVector) 0 else 1)
          }
        }
      }
    }
  }
}

case class EmptyBlockMatrixStage(eltType: Type) extends BlockMatrixStage(Array(), TInt32) {
  def blockContext(idx: (Int, Int)): IR =
    throw new LowererUnsupportedOperation("empty stage has no block contexts!")

  def blockBody(ctxRef: Ref): IR = NA(TNDArray(eltType, Nat(2)))

  override def collectBlocks(bindings: Seq[(String, Type)])(f: (IR, IR) => IR, blocksToCollect: Array[(Int, Int)]): IR = {
    assert(blocksToCollect.isEmpty)
    MakeArray(FastSeq(), TArray(f(Ref("x", ctxType), blockBody(Ref("x", ctxType))).typ))
  }
}

abstract class BlockMatrixStage(val globalVals: Array[(String, IR)], val ctxType: Type) {
  def blockContext(idx: (Int, Int)): IR

  def blockBody(ctxRef: Ref): IR

  def collectBlocks(bindings: Seq[(String, Type)])(f: (IR, IR) => IR, blocksToCollect: Array[(Int, Int)]): IR = {
    val ctxRef = Ref(genUID(), ctxType)
    val body = f(ctxRef, blockBody(ctxRef))
    val ctxs = MakeStream(blocksToCollect.map(idx => blockContext(idx)), TStream(ctxRef.typ))
    val bodyFreeVars = FreeVariables(body, supportsAgg = false, supportsScan = false)
    val bcFields = globalVals.filter { case (f, _) => bodyFreeVars.eval.lookupOption(f).isDefined }
    val bcVals = MakeStruct(bcFields.map { case (f, v) => f -> Ref(f, v.typ) } ++ bindings.map { case (name, t) => (name, Ref(name, t))})
    val bcRef = Ref(genUID(), bcVals.typ)
    val wrappedBody = bcFields.foldLeft(body) { case (accum, (f, _)) =>
      Let(f, GetField(bcRef, f), accum)
    }
    val collect = CollectDistributedArray(ctxs, bcVals, ctxRef.name, bcRef.name, wrappedBody)
    globalVals.foldRight[IR](collect) { case ((f, v), accum) => Let(f, v, accum) }
  }

  def collectLocal(bindings: Seq[(String, Type)], typ: BlockMatrixType): IR = {
    val blocksRowMajor = Array.range(0, typ.nRowBlocks).flatMap { i =>
      Array.tabulate(typ.nColBlocks)(j => i -> j).filter(typ.hasBlock)
    }
    val cda = collectBlocks(bindings)((_, b) => b, blocksRowMajor)
    val blockResults = Ref(genUID(), cda.typ)

    val rows = if (typ.isSparse) {
      val blockMap = blocksRowMajor.zipWithIndex.toMap
      MakeArray(Array.tabulate[IR](typ.nRowBlocks) { i =>
        NDArrayConcat(MakeArray(Array.tabulate[IR](typ.nColBlocks) { j =>
          if (blockMap.contains(i -> j))
            ArrayRef(blockResults, i * typ.nColBlocks + j)
          else {
            val (nRows, nCols) = typ.blockShape(i, j)
            MakeNDArray.fill(zero(typ.elementType), FastIndexedSeq(nRows, nCols), True())
          }
        }, coerce[TArray](cda.typ)), 1)
      }, coerce[TArray](cda.typ))
    } else {
      val i = Ref(genUID(), TInt32)
      val j = Ref(genUID(), TInt32)
      val cols = ToArray(StreamMap(StreamRange(0, typ.nColBlocks, 1), j.name, ArrayRef(blockResults, i * typ.nColBlocks + j)))
      ToArray(StreamMap(StreamRange(0, typ.nRowBlocks, 1), i.name, NDArrayConcat(cols, 1)))
    }
    Let(blockResults.name, cda, NDArrayConcat(rows, 0))
  }

  def addGlobals(newGlobals: (String, IR)*): BlockMatrixStage = {
    val outer = this
    new BlockMatrixStage(globalVals ++ newGlobals, ctxType) {
      def blockContext(idx: (Int, Int)): IR = outer.blockContext(idx)
      def blockBody(ctxRef: Ref): IR = outer.blockBody(ctxRef)
    }
  }

  def addContext(newTyp: Type)(newCtx: ((Int, Int)) => IR): BlockMatrixStage = {
    val outer = this
    val newCtxType = TStruct("old" -> ctxType, "new" -> newTyp)
    new BlockMatrixStage(globalVals, newCtxType) {
      def blockContext(idx: (Int, Int)): IR =
        makestruct("old" -> outer.blockContext(idx), "new" -> newCtx(idx))

      def blockBody(ctxRef: Ref): IR = bindIR(GetField(ctxRef, "old"))(outer.blockBody)
    }
  }
  def mapBody(f: (IR, IR) => IR): BlockMatrixStage = {
    val outer = this
    new BlockMatrixStage(globalVals, outer.ctxType) {
      def blockContext(idx: (Int, Int)): IR = outer.blockContext(idx)

      def blockBody(ctxRef: Ref): IR = f(ctxRef, outer.blockBody(ctxRef))
    }
  }
}

object LowerBlockMatrixIR {
  def apply(node: IR, typesToLower: DArrayLowering.Type, ctx: ExecuteContext, r: RequirednessAnalysis, relationalLetsAbove: Seq[(String, Type)]): IR = {

    def unimplemented[T](node: BaseIR): T =
      throw new LowererUnsupportedOperation(s"unimplemented: \n${ Pretty(node) }")

    def lowerIR(node: IR): IR = LowerToCDA.lower(node, typesToLower, ctx, r, relationalLetsAbove: Seq[(String, Type)])

    def lower(bmir: BlockMatrixIR): BlockMatrixStage = {
      if (!DArrayLowering.lowerBM(typesToLower))
        throw new LowererUnsupportedOperation("found BlockMatrixIR in lowering; lowering only TableIRs.")
      bmir.children.foreach {
        case c: BlockMatrixIR if c.typ.blockSize != bmir.typ.blockSize =>
          throw new LowererUnsupportedOperation(s"Can't lower node with mismatched block sizes: ${ bmir.typ.blockSize } vs child ${ c.typ.blockSize }\n\n ${ Pretty(bmir) }")
        case _ =>
      }
      if (bmir.typ.nDefinedBlocks == 0)
        BlockMatrixStage.empty(bmir.typ.elementType)
      else lowerNonEmpty(bmir)
    }

    def lowerNonEmpty(bmir: BlockMatrixIR): BlockMatrixStage = bmir match {
      case BlockMatrixRead(reader) => reader.lower(ctx)
      case x: BlockMatrixLiteral => unimplemented(bmir)
      case BlockMatrixMap(child, eltName, f, _) =>
        lower(child).mapBody { (_, body) =>
          NDArrayMap(body, eltName, f)
        }
      case BlockMatrixMap2(left, right, lname, rname, f, sparsityStrategy) =>
        val loweredLeft = lower(left)
        val loweredRight = lower(right)
        loweredLeft
          .addGlobals(loweredRight.globalVals: _*)
          .addContext(loweredRight.ctxType)(loweredRight.blockContext).mapBody { (ctx, leftBody) =>
          NDArrayMap2(leftBody, bindIR(GetField(ctx, "new"))(loweredRight.blockBody), lname, rname, f)
        }

      case x@BlockMatrixBroadcast(child, IndexedSeq(), _, _) =>
        val lowered = lower(child)
        val eltValue = lowered.globalVals.foldRight[IR](bindIR(lowered.blockContext(0 -> 0)) { ctx =>
          NDArrayRef(lowered.blockBody(ctx), FastIndexedSeq(I64(0L), I64(0L)))
        }) { case ((f, v), accum) => Let(f, v, accum) }

        val elt = Ref(genUID(), eltValue.typ)
        new BlockMatrixStage(Array(elt.name -> eltValue), TTuple(TInt64, TInt64)) {
          def blockContext(idx: (Int, Int)): IR = {
            val (i, j) = x.typ.blockShape(idx._1, idx._2)
            MakeTuple.ordered(FastSeq(I64(i), I64(j)))
          }
          def blockBody(ctxRef: Ref): IR =
            MakeNDArray(ToArray(mapIR(rangeIR(GetTupleElement(ctxRef, 0) * GetTupleElement(ctxRef, 1)))(_ => elt)),
              ctxRef, True())
        }
      case x@BlockMatrixBroadcast(child, IndexedSeq(axis), _, _) =>
        val len = child.typ.shape.max
        val vector = NDArrayReshape(lower(child).collectLocal(relationalLetsAbove, child.typ), MakeTuple.ordered(FastSeq(I64(len))))
        BlockMatrixStage.broadcastVector(vector, x.typ, asRowVector = axis == 1)

      case x@BlockMatrixBroadcast(child, IndexedSeq(axis, axis2), _, _) if (axis == axis2) => // diagonal as row/col vector
        val nBlocks = java.lang.Math.min(child.typ.nRowBlocks, child.typ.nColBlocks)
        val idxs = Array.tabulate(nBlocks) { i => i -> i }
        val getDiagonal = { (ctx: IR, block: IR) =>
          bindIR(block)(b => ToArray(mapIR(rangeIR(GetField(ctx, "new")))(i => NDArrayRef(b, FastIndexedSeq(i, i)))))
        }

        val vector = bindIR(lower(child).collectBlocks(relationalLetsAbove)(getDiagonal, idxs.filter(child.typ.hasBlock))) { existing =>
          var i = -1
          val vectorBlocks = idxs.map { idx =>
            if (child.typ.hasBlock(idx)) {
              i += 1
              ArrayRef(existing, i)
            } else {
              val (i, j) = child.typ.blockShape(idx._1, idx._2)
              mapIR(rangeIR(java.lang.Math.min(i, j)))(_ => zero(x.typ.elementType))
            }
          }
          MakeNDArray(ToArray(flatten(MakeStream(vectorBlocks, TStream(TStream(x.typ.elementType))))),
            MakeTuple.ordered(FastSeq(I64(java.lang.Math.min(child.typ.nRows, child.typ.nCols)))), true)
        }
        BlockMatrixStage.broadcastVector(vector, x.typ, asRowVector = axis == 1)

      case BlockMatrixBroadcast(child, IndexedSeq(1, 0), _, _) => //transpose
        val lowered = lower(child)
        new BlockMatrixStage(lowered.globalVals, lowered.ctxType) {
          def blockContext(idx: (Int, Int)): IR = lowered.blockContext(idx.swap)
          def blockBody(ctxRef: Ref): IR = NDArrayReindex(lowered.blockBody(ctxRef), FastIndexedSeq(1, 0))
        }
      case BlockMatrixBroadcast(child, IndexedSeq(0, 1), _, _) =>
        lower(child)

      case BlockMatrixAgg(child, outIndexExpr) => unimplemented(bmir)
      case BlockMatrixFilter(child, keep) => unimplemented(bmir)
      case BlockMatrixSlice(child, slices) => unimplemented(bmir)
      case BlockMatrixDensify(child) => unimplemented(bmir)
      case BlockMatrixSparsify(child, sparsifier) => unimplemented(bmir)
      case RelationalLetBlockMatrix(name, value, body) => unimplemented(bmir)
      case ValueToBlockMatrix(child, shape, blockSize) if !child.typ.isInstanceOf[TArray] =>
        throw new LowererUnsupportedOperation("use explicit broadcast for scalars!")
      case x@ValueToBlockMatrix(child, _, blockSize) => // row major or scalar
        val nd = MakeNDArray(child, MakeTuple.ordered(FastSeq(I64(x.typ.nRows), I64(x.typ.nCols))), True())
        val v = Ref(genUID(), nd.typ)
        new BlockMatrixStage(
          Array(v.name -> nd),
          nd.typ) {
          def blockContext(idx: (Int, Int)): IR = {
            val (r, c) = idx
            NDArraySlice(v, MakeTuple.ordered(FastSeq(
              MakeTuple.ordered(FastSeq(I64(r.toLong * blockSize), I64(java.lang.Math.min((r.toLong + 1) * blockSize, x.typ.nRows)), I64(1))),
              MakeTuple.ordered(FastSeq(I64(c.toLong * blockSize), I64(java.lang.Math.min((c.toLong + 1) * blockSize, x.typ.nCols)), I64(1))))))
          }

          def blockBody(ctxRef: Ref): IR = ctxRef
        }
      case x@BlockMatrixDot(leftIR, rightIR) =>
        val left = lower(leftIR)
        val right = lower(rightIR)
        val newCtxType = TArray(TTuple(left.ctxType, right.ctxType))
        new BlockMatrixStage(left.globalVals ++ right.globalVals, newCtxType) {
          def blockContext(idx: (Int, Int)): IR = {
            val (i, j) = idx
            MakeArray(Array.tabulate[Option[IR]](leftIR.typ.nColBlocks) { k =>
              if (leftIR.typ.hasBlock(i -> k) && rightIR.typ.hasBlock(k -> j))
                Some(MakeTuple.ordered(FastSeq(
                  left.blockContext(i -> k), right.blockContext(k -> j))))
              else None
            }.flatten[IR], newCtxType)
          }

          def blockBody(ctxRef: Ref): IR = {
            def blockMultiply(elt: Ref) =
              bindIR(GetTupleElement(elt, 0)) { leftElt =>
                bindIR(GetTupleElement(elt, 1)) { rightElt =>
                  NDArrayMatMul(left.blockBody(leftElt), right.blockBody(rightElt))
                }
              }
            foldIR(ToStream(invoke("sliceRight", ctxType, ctxRef, I32(1))),
              bindIR(ArrayRef(ctxRef, 0))(blockMultiply)) { (sum, elt) =>
              NDArrayMap2(sum, blockMultiply(elt), "l", "r",
                Ref("l", x.typ.elementType) + Ref("r", x.typ.elementType))
            }
          }
        }
    }

    node match {
      case BlockMatrixCollect(child) =>
        lower(child).collectLocal(relationalLetsAbove, child.typ)
      case BlockMatrixToValueApply(child, GetElement(IndexedSeq(i, j))) =>
        val rowBlock = child.typ.getBlockIdx(i)
        val colBlock = child.typ.getBlockIdx(j)

        val iInBlock = i - rowBlock * child.typ.blockSize
        val jInBlock = j - colBlock * child.typ.blockSize

        val lowered = lower(child)

        val elt = bindIR(lowered.blockContext(rowBlock -> colBlock)) { ctx =>
          NDArrayRef(lowered.blockBody(ctx), FastIndexedSeq(I64(iInBlock), I64(jInBlock)))
        }

        lowered.globalVals.foldRight[IR](elt) { case ((f, v), accum) => Let(f, v, accum) }
      case BlockMatrixWrite(child, writer) =>
        writer.lower(ctx, lower(child), child, relationalLetsAbove, TypeWithRequiredness(child.typ.elementType)) //FIXME: BlockMatrixIR is currently ignored in Requiredness inference since all eltTypes are +TFloat64
      case BlockMatrixMultiWrite(blockMatrices, writer) => unimplemented(node)
      case node if node.children.exists(_.isInstanceOf[BlockMatrixIR]) =>
        throw new LowererUnsupportedOperation(s"IR nodes with BlockMatrixIR children need explicit rules: \n${ Pretty(node) }")

      case node =>
        throw new LowererUnsupportedOperation(s"Value IRs with no BlockMatrixIR children must be lowered through LowerIR: \n${ Pretty(node) }")
    }
  }
}
