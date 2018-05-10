package is.hail.expr.ir.functions

import is.hail.annotations.StagedRegionValueBuilder
import is.hail.asm4s
import is.hail.asm4s._
import is.hail.expr.ir.EmitMethodBuilder
import is.hail.expr.types
import is.hail.expr.types._
import is.hail.utils.Interval
import is.hail.variant.{Locus, RGBase, ReferenceGenome, VariantMethods}

class ReferenceGenomeFunctions(rg: ReferenceGenome) extends RegistryFunctions {

  def rgCode(mb: EmitMethodBuilder): Code[ReferenceGenome] = mb.getReferenceGenome(rg)

  def emitLocus(mb: EmitMethodBuilder, locus: Code[Locus]): Code[Long] = {
    val srvb = new StagedRegionValueBuilder(mb, tlocus)
    Code(emitLocus(srvb, locus), srvb.offset)
  }

  def emitLocus(srvb: StagedRegionValueBuilder, locus: Code[Locus]): Code[Unit] = {
    val llocal = srvb.mb.newLocal[Locus]
    Code(
      llocal := locus,
      srvb.start(),
      srvb.addString(locus.invoke[String]("contig")),
      srvb.advance(),
      srvb.addInt(locus.invoke[Int]("position")),
      srvb.advance()
    )
  }

  def emitVariant(mb: EmitMethodBuilder, variant: Code[(Locus, IndexedSeq[String])]): Code[Long] = {
    val vlocal = mb.newLocal[(Locus, IndexedSeq[String])]
    val alocal = mb.newLocal[IndexedSeq[String]]
    val len = mb.newLocal[Int]
    val srvb = new StagedRegionValueBuilder(mb, tvariant)
    val addLocus = { srvb: StagedRegionValueBuilder =>
      emitLocus(srvb, Code.checkcast[Locus](vlocal.get[java.lang.Object]("_1")))
    }
    val addAlleles = { srvb: StagedRegionValueBuilder =>
      Code(
        srvb.start(len),
        Code.whileLoop(srvb.arrayIdx < len,
          srvb.addString(alocal.invoke[Int, String]("apply", srvb.arrayIdx)),
          srvb.advance()))
    }

    Code(
      vlocal := variant,
      alocal := Code.checkcast[IndexedSeq[String]](vlocal.get[java.lang.Object]("_2")),
      len := alocal.invoke[Int]("size"),
      srvb.start(),
      srvb.addBaseStruct(types.coerce[TStruct](tlocus.fundamentalType), addLocus),
      srvb.advance(),
      srvb.addArray(talleles, addAlleles),
      srvb.advance(),
      srvb.offset)
  }

  def emitInterval(mb: EmitMethodBuilder, interval: Code[Interval]): Code[Long] = {
    val ilocal = mb.newLocal[Interval]
    val plocal = mb.newLocal[Locus]
    val srvb = new StagedRegionValueBuilder(mb, tinterval)

    asm4s.coerce[Long](Code(
      ilocal := interval,
      srvb.start(),
      emitLocus(mb, Code.checkcast[Locus](ilocal.invoke[java.lang.Object]("start"))),
      srvb.advance(),
      emitLocus(mb, Code.checkcast[Locus](ilocal.invoke[java.lang.Object]("end"))),
      srvb.advance(),
      srvb.addBoolean(ilocal.invoke[Boolean]("includesStart")),
      srvb.advance(),
      srvb.addBoolean(ilocal.invoke[Boolean]("includesEnd")),
      srvb.advance(),
      srvb.offset))
  }

  var registered: Set[String] = Set[String]()

  val tlocus = TLocus(rg)
  val talleles = TArray(TString())
  val tvariant = TStruct("locus" -> tlocus, "alleles" -> talleles)
  val tinterval = TInterval(tlocus)

  def removeRegisteredFunctions(): Unit =
    registered.foreach(IRFunctionRegistry.removeIRFunction)

  def registerRGCode(
    mname: String, args: Array[Type], rt: Type, isDeterministic: Boolean)(
    impl: (EmitMethodBuilder, Array[Code[_]]) => Code[_]
  ): Unit = {
    val newName = rg.wrapFunctionName(mname)
    registered += newName
    registerCode(newName, args, rt, isDeterministic)(impl)
  }

  def registerRGCode(
    mname: String, arg1: Type, rt: Type, isDeterministic: Boolean)(
    impl: (EmitMethodBuilder, Code[_]) => Code[_]
  ): Unit =
    registerRGCode(mname, Array[Type](arg1), rt, isDeterministic) { case (mb, Array(a1)) => impl(mb, a1) }

  def registerRGCode(
    mname: String, arg1: Type, rt: Type)(
    impl: (EmitMethodBuilder, Code[_]) => Code[_]
  ): Unit = registerRGCode(mname, arg1, rt, isDeterministic = true)(impl)

  def registerRGCode(
    mname: String, arg1: Type, arg2: Type, rt: Type, isDeterministic: Boolean)(
    impl: (EmitMethodBuilder, Code[_], Code[_]) => Code[_]
  ): Unit =
    registerRGCode(mname, Array[Type](arg1, arg2), rt, isDeterministic) { case (mb, Array(a1, a2)) => impl(mb, a1, a2) }

  def registerRGCode(
    mname: String, arg1: Type, arg2: Type, rt: Type)(
    impl: (EmitMethodBuilder, Code[_], Code[_]) => Code[_]
  ): Unit = registerRGCode(mname, arg1, arg2, rt, isDeterministic = true)(impl)

  def registerRGCode(
    mname: String, arg1: Type, arg2: Type, arg3: Type, arg4: Type, arg5: Type, rt: Type, isDeterministic: Boolean)(
    impl: (EmitMethodBuilder, Code[_], Code[_], Code[_], Code[_], Code[_]) => Code[_]
  ): Unit =
    registerRGCode(mname, Array[Type](arg1, arg2, arg3, arg4, arg5), rt, isDeterministic) {
      case (mb, Array(a1, a2, a3, a4, a5)) => impl(mb, a1, a2, a3, a4, a5)
    }

  def registerRGCode(
    mname: String, arg1: Type, arg2: Type, arg3: Type, arg4: Type, arg5: Type, rt: Type)(
    impl: (EmitMethodBuilder, Code[_], Code[_], Code[_], Code[_], Code[_]) => Code[_]
  ): Unit = registerRGCode(mname, arg1, arg2, arg3, arg4, arg5, rt, isDeterministic = true)(impl)

  def registerAll() {

    val locusClass = Locus.getClass

    registerRGCode("Locus", TString(), TLocus(rg)) {
      case (mb, locusoff: Code[Long]) =>
        val slocus = asm4s.coerce[String](wrapArg(mb, TString())(locusoff))
        val locus = Code
          .invokeScalaObject[String, RGBase, Locus](
          locusClass, "parse", slocus, rgCode(mb))
        emitLocus(mb, locus)
    }

    registerRGCode("Locus", TString(), TInt32(), TLocus(rg)) {
      case (mb, contig: Code[Long], pos: Code[Int]) =>
        val scontig = asm4s.coerce[String](wrapArg(mb, TString())(contig))
        val locus = Code
          .invokeScalaObject[String, Int, RGBase, Locus](
          locusClass, "apply", scontig, pos, rgCode(mb))
        emitLocus(mb, locus)
    }

    registerRGCode("LocusAlleles", TString(), tvariant) {
      case (mb, variantoff: Code[Long]) =>
        val svar = asm4s.coerce[String](wrapArg(mb, TString())(variantoff))
        val variant = Code
          .invokeScalaObject[String, RGBase, (Locus, IndexedSeq[String])](
          VariantMethods.getClass, "parse", svar, rgCode(mb))
        emitVariant(mb, variant)
    }

    registerRGCode("LocusInterval", TString(), tinterval) {
      case (mb, ioff: Code[Long]) =>
        val sinterval = asm4s.coerce[String](wrapArg(mb, TString())(ioff))
        val interval = Code
          .invokeScalaObject[String, RGBase, Interval](
          locusClass, "parseInterval", sinterval, rgCode(mb))
        emitInterval(mb, interval)
    }

    registerRGCode("LocusInterval", TString(), TInt32(), TInt32(), TBoolean(), TBoolean(), tinterval) {
      case (mb, locoff: Code[Long], pos1: Code[Int], pos2: Code[Int], include1: Code[Boolean], include2: Code[Boolean]) =>
        val sloc = asm4s.coerce[String](wrapArg(mb, TString())(locoff))
        val interval = Code
          .invokeScalaObject[String, Int, Int, Boolean, Boolean, RGBase, Interval](
          locusClass, "makeInterval", sloc, pos1, pos2, include1, include2, rgCode(mb))
        emitInterval(mb, interval)
    }
  }
}
