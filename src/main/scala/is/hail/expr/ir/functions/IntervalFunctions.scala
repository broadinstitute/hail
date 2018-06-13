package is.hail.expr.ir.functions

import is.hail.annotations.{CodeOrdering, Region, StagedRegionValueBuilder}
import is.hail.asm4s
import is.hail.asm4s._
import is.hail.expr.ir._
import is.hail.expr.types._
import is.hail.utils._

object IntervalFunctions extends RegistryFunctions {

  def registerAll(): Unit = {

    registerCodeWithMissingness("Interval", tv("T"), tv("T"), TBoolean(), TBoolean(), TInterval(tv("T"))) {
      (mb, start, end, includeStart, includeEnd) =>
        val srvb = new StagedRegionValueBuilder(mb, TInterval(tv("T").t))
        val missing = includeStart.m || includeEnd.m
        val value = Code(
          srvb.start(),
          start.m.mux(
            srvb.setMissing(),
            srvb.addIRIntermediate(tv("T").t)(start.v)),
          srvb.advance(),
          end.m.mux(
            srvb.setMissing(),
            srvb.addIRIntermediate(tv("T").t)(end.v)),
          srvb.advance(),
          srvb.addBoolean(includeStart.value[Boolean]),
          srvb.advance(),
          srvb.addBoolean(includeEnd.value[Boolean]),
          srvb.advance(),
          srvb.offset
        )

        EmitTriplet(
          Code(start.setup, end.setup, includeStart.setup, includeEnd.setup),
          missing,
          value)
    }

    registerCodeWithMissingness("start", TInterval(tv("T")), tv("T")) {
      case (mb, interval) =>
        val tinterval = TInterval(tv("T").t)
        val region = mb.getArg[Region](1).load()
        val iv = mb.newLocal[Long]
        EmitTriplet(
          interval.setup,
          interval.m || !Code(iv := interval.value[Long], tinterval.startDefined(region, iv)),
          region.loadIRIntermediate(tv("T").t)(tinterval.startOffset(iv))
        )
    }

    registerCodeWithMissingness("end", TInterval(tv("T")), tv("T")) {
      case (mb, interval) =>
        val tinterval = TInterval(tv("T").t)
        val region = mb.getArg[Region](1).load()
        val iv = mb.newLocal[Long]
        EmitTriplet(
          interval.setup,
          interval.m || !Code(iv := interval.value[Long], tinterval.endDefined(region, iv)),
          region.loadIRIntermediate(tv("T").t)(tinterval.endOffset(iv))
        )
    }

    registerCode("includesStart", TInterval(tv("T")), TBooleanOptional) {
      case (mb, interval: Code[Long]) =>
        val region = mb.getArg[Region](1).load()
        TInterval(tv("T").t).includeStart(region, interval)
    }

    registerCode("includesEnd", TInterval(tv("T")), TBooleanOptional) {
      case (mb, interval: Code[Long]) =>
        val region = mb.getArg[Region](1).load()
        TInterval(tv("T").t).includeEnd(region, interval)
    }

    registerCodeWithMissingness("contains", TInterval(tv("T")), tv("T"), TBoolean()) {
      case (mb, intTriplet, pointTriplet) =>
        val pointType = tv("T").t

        val mPoint = mb.newLocal[Boolean]
        val vPoint = mb.newLocal()(typeToTypeInfo(pointType))

        val cmp = mb.newLocal[Int]
        val interval = new IRInterval(mb, TInterval(pointType), intTriplet.value[Long])
        val compare = interval.ordering(CodeOrdering.compare)

        val contains = Code(
          interval.storeToLocal,
          mPoint := pointTriplet.m,
          vPoint.storeAny(pointTriplet.v),
          cmp := compare(interval.start, (mPoint, vPoint)),
          (cmp > 0 || (cmp.ceq(0) && interval.includeStart)) && Code(
            cmp := compare((mPoint, vPoint), interval.end),
            cmp < 0 || (cmp.ceq(0) && interval.includeEnd)))

        EmitTriplet(
          Code(intTriplet.setup, pointTriplet.setup),
          intTriplet.m,
          contains)
    }

    registerCode("isEmpty", TInterval(tv("T")), TBoolean()) {
      case (mb, intOff) =>
        val interval = new IRInterval(mb, TInterval(tv("T").t), intOff)

        Code(
          interval.storeToLocal,
          interval.isEmpty
        )
    }

    registerCode("overlaps", TInterval(tv("T")), TInterval(tv("T")), TBoolean()) {
      case (mb, iOff1, iOff2) =>
        val pointType = tv("T").t

        val interval1 = new IRInterval(mb, TInterval(pointType), iOff1)
        val interval2 = new IRInterval(mb, TInterval(pointType), iOff2)

        Code(
          interval1.storeToLocal,
          interval2.storeToLocal,
          !(interval1.isEmpty || interval2.isEmpty ||
            interval1.isBelow(interval2) || interval1.isAbove(interval2))
        )
    }
  }
}

class IRInterval(mb: EmitMethodBuilder, typ: TInterval, value: Code[Long]) {
  val ref: LocalRef[Long] = mb.newLocal[Long]
  val region: Code[Region] = IntervalFunctions.getRegion(mb)

  def ordering[T](op: CodeOrdering.Op): ((Code[Boolean], Code[_]), (Code[Boolean], Code[_])) => Code[T] =
    mb.getCodeOrdering[T](typ.pointType, op, missingGreatest = true)(region, _, region, _)

  def storeToLocal: Code[Unit] = ref := value

  def start: (Code[Boolean], Code[_]) =
    (!typ.startDefined(region, ref), region.getIRIntermediate(typ.pointType)(typ.startOffset(ref)))
  def end: (Code[Boolean], Code[_]) =
    (!typ.endDefined(region, ref), region.getIRIntermediate(typ.pointType)(typ.endOffset(ref)))
  def includeStart: Code[Boolean] = typ.includeStart(region, ref)
  def includeEnd: Code[Boolean] = typ.includeEnd(region, ref)

  def isEmpty: Code[Boolean] = {
    val gt = ordering(CodeOrdering.gt)
    val gteq = ordering(CodeOrdering.gteq)

    (includeStart && includeEnd).mux(
      gt(start, end),
      gteq(start, end))
  }

  def isAbove(other: IRInterval): Code[Boolean]
  def isBelow(other: IRInterval): Code[Boolean]
}
