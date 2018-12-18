package is.hail.nativecode

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import is.hail.SparkSuite
import is.hail.annotations._
import is.hail.cxx._
import is.hail.expr.types.virtual._
import is.hail.io._
import is.hail.rvd.RVDContext
import is.hail.sparkextras.ContextRDD
import is.hail.utils._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.testng.annotations.Test

class RegionValueIteratorSuite extends SparkSuite {

  def getData(): (BufferSpec, TTuple, IndexedSeq[Row]) = {
    val spec = new LEB128BufferSpec(
      new BlockingBufferSpec(32,
        new LZ4BlockBufferSpec(32,
          new StreamBlockBufferSpec)))
    val t = TTuple(TInterval(TStruct("x"->TSet(TInt32()))))

    val a = FastIndexedSeq(
      Row(Interval(Row(Set(-1478292367)), Row(Set(2084728308)), true, true)),
      Row(Interval(Row(Set(-1)), Row(Set(2346211)), true, false)),
      Row(Interval(Row(Set(25)), Row(Set(70)), false, true)),
      Row(Interval(Row(Set(50)), Row(Set(48)), false, false)))

    (spec, t, a)
  }

  @Test def testScalaRegionValueIterator(): Unit = {
    hc
    val (spec, t, a) = getData()
    val decMod = PackDecoder.buildModule(t.physicalType, t.physicalType, spec)

    val tub = new TranslationUnitBuilder
    val encClass = PackEncoder(t.physicalType, spec, tub)

    tub.include("<jni.h>")
    tub.include("hail/PartitionIterators.h")
    val partitionFB = tub.buildFunction("partition_f", Array("NativeStatus*" -> "st", "long" -> "objects"), "long")
    val up = tub.variable("up", "UpcallEnv")
    val encoder = tub.variable("encoder", encClass.name, s"std::make_shared<OutputStream>($up, reinterpret_cast<ObjectArray * >(${ partitionFB.getArg(1) })->at(1))")
    val jit = tub.variable("jit", "JavaIteratorObject", s"JavaIteratorObject($up, reinterpret_cast<ObjectArray * >(${ partitionFB.getArg(1) })->at(0))")

    partitionFB +=
      s"""
         |${ up.define }
         |${ encoder.define }
         |${ jit.define }
         |for(auto it : $jit) {
         |  $encoder.encode_row(it);
         |}
         |$encoder.flush();
         |return 0;
       """.stripMargin
    partitionFB.end()

    val mod = tub.end().build("-O2 -llz4")
    val key = mod.getKey
    val bin = mod.getBinary

    val result = hc.sc.parallelize(a, 2).mapPartitions { rowsIt =>
      val rows = rowsIt.toIndexedSeq
      Region.scoped { region =>
        val rvb = new RegionValueBuilder(region)
        val it = new RegionValueIterator(rows.toIterator.map { r =>
          rvb.start(t.physicalType)
          rvb.addAnnotation(t, r)
          RegionValue(region, rvb.end())
        })

        val baos = new ByteArrayOutputStream()
        val obj = new ObjectArray(it, baos)

        val st = new NativeStatus()
        val mod = new NativeModule(key, bin)
        val f = mod.findLongFuncL1(st, "partition_f")
        f(st, obj.get())

        val bais = new ByteArrayInputStream(baos.toByteArray)
        val decoder = new NativePackDecoder(bais, decMod)

        rows.map { r =>
          SafeRow(t.asInstanceOf[TBaseStruct].physicalType, region, decoder.readRegionValue(region))
        }.toIterator
      }
    }.collect()

    assert(result sameElements a)
  }

  @Test def testCXXRegionValueIterator(): Unit = {
    val (spec, t, a) = getData()

    val encMod = PackEncoder.buildModule(t.physicalType, spec)
    val tub = new TranslationUnitBuilder()
    val decClass = PackDecoder(t.physicalType, t.physicalType, spec, tub)
    tub.include("hail/PartitionIterators.h")
    tub.include("hail/ObjectArray.h")
    val makeItF = tub.buildFunction("make_iterator", Array("NativeStatus *"->"st", "long" -> "reg", "long" -> "obj"), "NativeObjPtr")

    val cb = tub.buildClass("CXXIterator", "NativeObj")
    val dec = tub.variable("dec", decClass.name)
    val reg = tub.variable("reg", "ScalaRegion *")
    val value = tub.variable("value", "mutable char const *", "nullptr")
    cb += dec
    cb += reg
    cb += value
    cb +=
      s"""${cb.name}(jobject is, ScalaRegion * reg) :
         |$dec(std::make_shared<InputStream>(UpcallEnv(), is)), $reg(reg) {
         |  $value = ($dec.decode_byte()) ? $dec.decode_row($reg->get_wrapped_region()) : nullptr;
         |}
         """.stripMargin

    cb += new Function("bool", "advance", Array(),
      s"$value = ($dec.decode_byte()) ? $dec.decode_row($reg->get_wrapped_region()) : nullptr; return ($value == nullptr);", const = true)
    cb += new Function("char const*", "get", Array(), s"return $value;", const = true)

    val itclass = cb.buildClass("Iterator")
    val range = tub.variable("range", s"const ${ cb.name } *")
    itclass += range
    itclass += s"Iterator(const ${ cb.name } * range) : ${ range }(range) { } "
    itclass += new Function(s"Iterator&", "operator++", Array(), s"if ($range->advance()) { $range = nullptr; } return *this;")
    itclass += new Function("char const *", "operator*", Array(), s"return $range->get();", const = true)
    itclass += new Function("friend bool", "operator==", Array(Variable("lhs", "const Iterator &"), Variable("rhs", "const Iterator &")), s"return (lhs.$range == rhs.$range);")
    itclass += new Function("friend bool", "operator!=", Array(Variable("lhs", "const Iterator &"), Variable("rhs", "const Iterator &")), s"return !(lhs == rhs);")
    itclass.end()
    cb += new Function(s"Iterator", "begin", Array(), s"return Iterator(this);", const=true)
    cb += new Function(s"Iterator", "end", Array(), s"return Iterator(nullptr);", const=true)
    cb.end()

    val itType = cb.name
    makeItF += s"return std::make_shared<ScalaStagingIterator<$itType>>(reinterpret_cast<ObjectArray *>(${ makeItF.getArg(2) })->at(0), reinterpret_cast<ScalaRegion *>(${ makeItF.getArg(1) }));"
    makeItF.end()

    val modToPtr = { (mod: NativeModule, region: Region, obj: ObjectArray) =>
      val st = new NativeStatus()
      val ptrF = mod.findPtrFuncL2(st, "make_iterator")
      scala.Predef.assert(st.ok, st.toString())
      val ptr = new NativePtr(ptrF, st, region.get(), obj.get())
      scala.Predef.assert(st.ok, st.toString())
      ptrF.close()
      st.close()
      obj.close()
      ptr
    }

    val makeIt = CXXRegionValueIterator(s"ScalaStagingIterator<$itType>", tub, modToPtr)

    val encoded = hc.sc.parallelize(a, 2).mapPartitions { case (rowsIt) =>
      val rows = rowsIt.toIndexedSeq
      Region.scoped { region =>
        val rvb = new RegionValueBuilder(region)
        val baos = new ByteArrayOutputStream()
        val encoder = new NativePackEncoder(baos, encMod)
        rows.foreach { r =>
          rvb.start(t.physicalType)
          rvb.addAnnotation(t, r)
          encoder.writeByte(1)
          encoder.writeRegionValue(region, rvb.end())
        }
        encoder.writeByte(0)
        encoder.flush()
        Iterator.single(baos.toByteArray)
      }
    }

    val result = ContextRDD.weaken[RVDContext](encoded).cmapPartitions { case (ctx, it) =>
      makeIt(ctx.region, new ObjectArray(new ByteArrayInputStream(it.next())))
    }.map(SafeRow(t.asInstanceOf[TBaseStruct].physicalType, _)).collect()

    assert(result sameElements a)
  }
}
