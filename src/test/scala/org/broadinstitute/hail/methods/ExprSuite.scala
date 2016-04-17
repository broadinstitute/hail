package org.broadinstitute.hail.methods

import org.broadinstitute.hail.{FatalException, SparkSuite}
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.annotations.Annotation
import org.broadinstitute.hail.check.Prop._
import org.broadinstitute.hail.driver._
import org.broadinstitute.hail.expr._
import org.broadinstitute.hail.variant.Genotype
import org.testng.annotations.Test

import scala.collection.mutable.ArrayBuffer

class ExprSuite extends SparkSuite {

  @Test def exprTest() {
    val symTab = Map("i" ->(0, TInt),
      "j" ->(1, TInt),
      "d" ->(2, TDouble),
      "d2" ->(3, TDouble),
      "s" ->(4, TString),
      "s2" ->(5, TString),
      "a" ->(6, TArray(TInt)),
      "m" ->(7, TInt),
      "as" ->(8, TArray(TStruct(("a", TInt),
        ("b", TString)))),
      "gs" ->(9, TStruct(("noCall", TGenotype),
        ("homRef", TGenotype),
        ("het", TGenotype),
        ("homVar", TGenotype),
        ("hetNonRef35", TGenotype))),
      "t" ->(10, TBoolean),
      "f" ->(11, TBoolean),
      "mb" ->(12, TBoolean),
      "is" ->(13, TString))
    val a = new ArrayBuffer[Any]()
    a += 5 // i
    a += -7 // j
    a += 3.14
    a += 5.79e7
    a += "12,34,56,78"
    a += "this is a String, there are many like it, but this one is mine"
    a += IndexedSeq(1, 2, null, 6, 3, 3, -1, 8)
    a += null // m
    a += (Array[Any](Annotation(23, "foo"),
      Annotation(-7, null)): IndexedSeq[Any])
    a += Annotation(
      Genotype(),
      Genotype(gt = Some(0)),
      Genotype(gt = Some(1)),
      Genotype(gt = Some(2)),
      Genotype(gt = Some(Genotype.gtIndex(3, 5))))
    a += true
    a += false
    a += null // mb
    a += "-37" // is
    assert(a.length == 14)

    def eval[T](s: String): Option[T] = {
      val f = Parser.parse(s, symTab, a)._2
      f().map(_.asInstanceOf[T])
    }

    assert(eval[Int]("is.toInt").contains(-37))

    assert(eval[Boolean]("!gs.het.isHomRef").contains(true))

    assert(eval[Boolean]("(1 / 2) == 0.5").contains(true))
    assert(eval[Boolean]("(1.0 / 2.0) == 0.5").contains(true))
    assert(eval[Boolean]("(1 / 2.0) == 0.5").contains(true))
    assert(eval[Boolean]("(1.0 / 2) == 0.5").contains(true))

    assert(eval[Boolean]("isMissing(gs.noCall.gt)").contains(true))
    assert(eval[Boolean]("gs.noCall.gt").isEmpty)

    assert(eval[Boolean]("isMissing(gs.noCall.gtj)").contains(true))
    assert(eval[Boolean]("gs.noCall.gtj").isEmpty)

    assert(eval[Boolean]("isMissing(gs.noCall.gtk)").contains(true))
    assert(eval[Boolean]("gs.noCall.gtk").isEmpty)

    assert(eval[Int]("let a = i and b = j in a + b").contains(-2))
    assert(eval[Int]("let a = i and b = a + j in b").contains(-2))
    assert(eval[Int]("let i = j in i").contains(-7))
    assert(eval[Int]("let a = let b = j in b + 1 in a + 1").contains(-5))

    assert(eval[Boolean]("mb || true").contains(true))
    assert(eval[Boolean]("true || mb").contains(true))
    assert(eval[Boolean]("isMissing(false || mb)").contains(true))
    assert(eval[Boolean]("isMissing(mb || false)").contains(true))

    assert(eval[Int]("gs.homRef.gtj").contains(0)
      && eval[Int]("gs.homRef.gtk").contains(0))
    assert(eval[Int]("gs.het.gtj").contains(0)
      && eval[Int]("gs.het.gtk").contains(1))
    assert(eval[Int]("gs.homVar.gtj").contains(1)
      && eval[Int]("gs.homVar.gtk").contains(1))
    assert(eval[Int]("gs.hetNonRef35.gtj").contains(3)
      && eval[Int]("gs.hetNonRef35.gtk").contains(5))

    assert(eval[Int]("i.orElse(3)").contains(5))
    assert(eval[Int]("m.orElse(3)").contains(3))

    assert(eval[Boolean]("isMissing(i)").contains(false))
    assert(eval[Boolean]("isDefined(i)").contains(true))
    assert(eval[Boolean]("isDefined(i)").contains(true))
    assert(eval[Boolean]("i").nonEmpty)

    assert(eval[Boolean]("isMissing(m)").contains(true))
    assert(eval[Boolean]("isDefined(m)").contains(false))
    assert(eval[Boolean]("m").isEmpty)

    assert(eval[Boolean]("isMissing(a[1])").contains(false))
    assert(eval[Boolean]("isDefined(a[1])").contains(true))
    assert(eval[Boolean]("a[1]").nonEmpty)

    assert(eval[Boolean]("isMissing(a[2])").contains(true))
    assert(eval[Boolean]("isDefined(a[2])").contains(false))
    assert(eval[Boolean]("a[2]").isEmpty)

    assert(eval[Int]("as.length").contains(2))
    assert(eval[Int]("as[0].a").contains(23))
    assert(eval[Boolean]("isMissing(as[1].b)").contains(true))
    assert(eval[Boolean]("as[1].b").isEmpty)

    assert(eval[Int]("i").contains(5))
    assert(eval[Int]("j").contains(-7))
    assert(eval[Int]("i.max(j)").contains(5))
    assert(eval[Int]("i.min(j)").contains(-7))
    assert(eval[Double]("d").exists(D_==(_, 3.14)))
    assert(eval[IndexedSeq[String]]("""s.split(",")""").contains(IndexedSeq("12", "34", "56", "78")))
    assert(eval[Int]("s2.length").contains(62))

    assert(eval[Int]("""a.find(x => x < 0)""").contains(-1))

    // FIXME catch parse errors
    // assert(eval[Boolean]("i.max(d) == 5"))
  }

  @Test def testParseTypes() {
    val s1 = "SIFT_Score: Double, Age: Int"
    val s2 = ""
    val s3 = "SIFT_Score: Double, Age: Int, SIFT2: BadType"

    assert(Parser.parseAnnotationTypes(s1) == Map("SIFT_Score" -> TDouble, "Age" -> TInt))
    assert(Parser.parseAnnotationTypes(s2) == Map.empty[String, BaseType])
    intercept[FatalException](Parser.parseAnnotationTypes(s3) == Map("SIFT_Score" -> TDouble, "Age" -> TInt))
  }

  @Test def testTypePretty() {
    import Type._
    // for arbType

    val sb = new StringBuilder
    check(forAll { (t: Type) =>
      sb.clear()
      t.pretty(sb, 0, printAttrs = true)
      val res = sb.result()
      val parsed = Parser.parseType(res)
      t == parsed
    })
  }

  @Test def testJSON() {
    check(forAll { (t: Type) =>
      val a = t.genValue.sample()
      val json = t.makeJSON(a)
      a == VEP.jsonToAnnotation(json, t, "")
    })
  }
}
