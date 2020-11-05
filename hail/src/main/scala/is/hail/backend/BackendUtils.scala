package is.hail.backend

import is.hail.HailContext
import is.hail.annotations.{Region, RegionPool}
import is.hail.asm4s._
import is.hail.expr.ir.lowering.TableStageDependency

object BackendUtils {
  type F = AsmFunction3[Region, Array[Byte], Array[Byte], Array[Byte]]
}

class BackendUtils(mods: Array[(String, (Int, Region) => BackendUtils.F)]) {
  import BackendUtils.F

  private[this] val loadedModules: Map[String, (Int, Region) => F] = mods.toMap

  def getModule(id: String): (Int, Region) => F = loadedModules(id)

  def collectDArray(backendContext: BackendContext, modID: String, contexts: Array[Array[Byte]], globals: Array[Byte], tsd: Option[TableStageDependency]): Array[Array[Byte]] = {
    if (contexts.isEmpty)
      return Array()
    val backend = HailContext.backend
    val globalsBC = backend.broadcast(globals)
    val f = getModule(modID)

    backend.parallelizeAndComputeWithIndex(backendContext, contexts, tsd) { (ctx, i) =>
      val gs = globalsBC.value
      RegionPool.get.scopedRegion { region =>
        val res = f(i, region)(region, ctx, gs)
        res
      }
    }
  }
}
