package is.hail.nativecode

import is.hail.utils.fatal
import org.apache.spark.TaskContext

// NativeModule refers to a single DLL.
//
// It may be constructed either with compile-options and source-text,
// or with a module "key", which is is a 20-hex-digit hash of the 
// (options, sourceText).  
//
// On the master node, the DLL file may already exist, or it may be 
// generated by compiling the source code and writing the file.
//
// On a worker node, the binary of the DLL file will be provided
// as Array[Byte].

class NativeModule() extends NativeBase() {
  @native def nativeCtorMaster(options: String, source: String, inc: String): Unit
  @native def nativeCtorWorker(isGlobal: Boolean, key: String, binary: Array[Byte]): Unit

  // Copy-constructor
  def this(b: NativeModule) {
    this()
    super.copyAssign(b)
  }

  // Constructor with master parameters only
  def this(options: String, source: String) {
    this()
    // on master
    assert(TaskContext.get() == null)
    val includeDir = NativeCode.getIncludeDir
    nativeCtorMaster(options, source, includeDir)
  }

  // Constructor with worker parameters only  
  def this(key: String, binary: Array[Byte]) {
    this()
    nativeCtorWorker(isGlobal = false, key, binary)
  }

  // Constructor for fake module "global" to find all funcs in program
  def this(fakeKeyGlobal: String) {
    this()
    nativeCtorWorker(isGlobal = true, "global", Array[Byte]())
  }

  def copyAssign(b: NativeModule): Unit = super.copyAssign(b)
  def moveAssign(b: NativeModule): Unit = super.moveAssign(b)
  
  //
  // Compilation and DLL file reading/writing
  //    
  @native def nativeFindOrBuild(st: Long): Unit
  
  def findOrBuild(st: NativeStatus): Unit = nativeFindOrBuild(st.get())

  def findOrBuild(): Unit = {
    val st = new NativeStatus()
    findOrBuild(st)
    assert(st.ok, st.toString())
  }

  //
  // Methods needed for sending module to workers
  //
  @native def getKey: String

  @native def getBinary: Array[Byte]

  // Once we have a NativeModule, we can find particular funcs and makers
  // We pass in an unmangled C++ function name, which should be declared
  // inside namespaces hail::HAIL_MODULE (where HAIL_MODULE will be
  // defined by build options e.g. "-DHAIL_MODULE=module_98a53267bdfee2c9d126")
  
  @native def nativeFindLongFuncL0(st: Long, func: NativeLongFuncL0, name: String): Unit
  @native def nativeFindLongFuncL1(st: Long, func: NativeLongFuncL1, name: String): Unit
  @native def nativeFindLongFuncL2(st: Long, func: NativeLongFuncL2, name: String): Unit
  @native def nativeFindLongFuncL3(st: Long, func: NativeLongFuncL3, name: String): Unit
  @native def nativeFindLongFuncL4(st: Long, func: NativeLongFuncL4, name: String): Unit
  @native def nativeFindLongFuncL5(st: Long, func: NativeLongFuncL5, name: String): Unit
  @native def nativeFindLongFuncL6(st: Long, func: NativeLongFuncL6, name: String): Unit
  @native def nativeFindLongFuncL7(st: Long, func: NativeLongFuncL7, name: String): Unit
  @native def nativeFindLongFuncL8(st: Long, func: NativeLongFuncL8, name: String): Unit
  
  def findLongFuncL0(st: NativeStatus, name: String): NativeLongFuncL0 = {
    val func = new NativeLongFuncL0()
    nativeFindLongFuncL0(st.get(), func, name)
    func
  }
  
  def findLongFuncL1(st: NativeStatus, name: String): NativeLongFuncL1 = {
    val func = new NativeLongFuncL1()
    nativeFindLongFuncL1(st.get(), func, name)
    func
  }
  
  def findLongFuncL2(st: NativeStatus, name: String): NativeLongFuncL2 = {
    val func = new NativeLongFuncL2()
    nativeFindLongFuncL2(st.get(), func, name)
    func
  }
  
  def findLongFuncL3(st: NativeStatus, name: String): NativeLongFuncL3 = {
    val func = new NativeLongFuncL3()
    nativeFindLongFuncL3(st.get(), func, name)
    func
  }

  def findLongFuncL3(name: String): (Long, Long, Long) => Long = {
    val st = new NativeStatus()
    val f = findLongFuncL3(st, name)
    assert(st.ok, st.toString())
    val wrapped = { (v1: Long, v2: Long, v3: Long) =>
      val res = f(st, v1, v2, v3)
      if (st.fail)
        fatal(st.toString())
      res
    }
    wrapped
  }
  
  def findLongFuncL4(st: NativeStatus, name: String): NativeLongFuncL4 = {
    val func = new NativeLongFuncL4()
    nativeFindLongFuncL4(st.get(), func, name)
    func
  }

  def findLongFuncL4(name: String): (Long, Long, Long, Long) => Long = {
    val st = new NativeStatus()
    val f = findLongFuncL4(st, name)
    assert(st.ok, st.toString())
    val wrapped = { (v1: Long, v2: Long, v3: Long, v4: Long) =>
      val res = f(st, v1, v2, v3, v4)
      if (st.fail)
        fatal(st.toString())
      res
    }
    wrapped
  }
  
  def findLongFuncL5(st: NativeStatus, name: String): NativeLongFuncL5 = {
    val func = new NativeLongFuncL5()
    nativeFindLongFuncL5(st.get(), func, name)
    func
  }
  
  def findLongFuncL6(st: NativeStatus, name: String): NativeLongFuncL6 = {
    val func = new NativeLongFuncL6()
    nativeFindLongFuncL6(st.get(), func, name)
    func
  }
  
  def findLongFuncL7(st: NativeStatus, name: String): NativeLongFuncL7 = {
    val func = new NativeLongFuncL7()
    nativeFindLongFuncL7(st.get(), func, name)
    func
  }
  
  def findLongFuncL8(st: NativeStatus, name: String): NativeLongFuncL8 = {
    val func = new NativeLongFuncL8()
    nativeFindLongFuncL8(st.get(), func, name)
    func
  }
  
  @native def nativeFindPtrFuncL0(st: Long, func: NativePtrFuncL0, name: String): Unit
  @native def nativeFindPtrFuncL1(st: Long, func: NativePtrFuncL1, name: String): Unit
  @native def nativeFindPtrFuncL2(st: Long, func: NativePtrFuncL2, name: String): Unit
  @native def nativeFindPtrFuncL3(st: Long, func: NativePtrFuncL3, name: String): Unit
  @native def nativeFindPtrFuncL4(st: Long, func: NativePtrFuncL4, name: String): Unit
  @native def nativeFindPtrFuncL5(st: Long, func: NativePtrFuncL5, name: String): Unit
  @native def nativeFindPtrFuncL6(st: Long, func: NativePtrFuncL6, name: String): Unit
  @native def nativeFindPtrFuncL7(st: Long, func: NativePtrFuncL7, name: String): Unit
  @native def nativeFindPtrFuncL8(st: Long, func: NativePtrFuncL8, name: String): Unit
  
  def findPtrFuncL0(st: NativeStatus, name: String): NativePtrFuncL0 = {
    val func = new NativePtrFuncL0()
    nativeFindPtrFuncL0(st.get(), func, name)
    func
  }
  
  def findPtrFuncL1(st: NativeStatus, name: String): NativePtrFuncL1 = {
    val func = new NativePtrFuncL1()
    nativeFindPtrFuncL1(st.get(), func, name)
    func
  }
  
  def findPtrFuncL2(st: NativeStatus, name: String): NativePtrFuncL2 = {
    val func = new NativePtrFuncL2()
    nativeFindPtrFuncL2(st.get(), func, name)
    func
  }
  
  def findPtrFuncL3(st: NativeStatus, name: String): NativePtrFuncL3 = {
    val func = new NativePtrFuncL3()
    nativeFindPtrFuncL3(st.get(), func, name)
    func
  }
  
  def findPtrFuncL4(st: NativeStatus, name: String): NativePtrFuncL4 = {
    val func = new NativePtrFuncL4()
    nativeFindPtrFuncL4(st.get(), func, name)
    func
  }
  
  def findPtrFuncL5(st: NativeStatus, name: String): NativePtrFuncL5 = {
    val func = new NativePtrFuncL5()
    nativeFindPtrFuncL5(st.get(), func, name)
    func
  }
  
  def findPtrFuncL6(st: NativeStatus, name: String): NativePtrFuncL6 = {
    val func = new NativePtrFuncL6()
    nativeFindPtrFuncL6(st.get(), func, name)
    func
  }
  
  def findPtrFuncL7(st: NativeStatus, name: String): NativePtrFuncL7 = {
    val func = new NativePtrFuncL7()
    nativeFindPtrFuncL7(st.get(), func, name)
    func
  }
  
  def findPtrFuncL8(st: NativeStatus, name: String): NativePtrFuncL8 = {
    val func = new NativePtrFuncL8()
    nativeFindPtrFuncL8(st.get(), func, name)
    func
  }

}
