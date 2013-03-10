//----------------------------------------------------------------------------
//
// Copyright (c) 2013 The Tranq Team. 
//
// This source code is subject to terms and conditions of the Apache License, Version 2.0. A 
// copy of the license can be found in the License.txt file at the root of this distribution. 
// By using this source code in any fashion, you are agreeing to be bound 
// by the terms of the Apache License, Version 2.0.
//
// You must not remove this notice, or any other, from this software.
//----------------------------------------------------------------------------

namespace Tranq.Test.MsSql

open System
open NUnit.Framework
open Tranq

module CallTest = 

  type Department =
    { [<Id>]
      DepartmentId : int
      DepartmentName : string
      [<Version>]
      VersionNo : int }

  type Employee =
    { [<Id(IdKind.Identity)>]
      EmployeeId : int option
      EmployeeName : string option
      DepartmentId : int option
      [<Version>]
      VersionNo : int option }

  type ProcNoneParam = 
    { Unit : unit }

  type ProcSingleParam =
    { Param1 : int }

  type ProcMultiParams =
    { Param1 : int
      [<ProcedureParam(Name = "Param2", Direction = Direction.InputOutput)>]
      Hoge : int
      [<ProcedureParam(Direction = Direction.Output)>]
      Param3 : int }

  [<Procedure(Name = "ProcResult")>]
  type ProcEntityResult =
    { EmployeeId : int
      [<ProcedureParam(Direction = Direction.Result)>]
      EmpList : Employee list }

  [<Procedure(Name = "ProcResult")>]
  type ProcTupleResult =
    { EmployeeId : int
      [<ProcedureParam(Direction = Direction.Result)>]
      EmpList : (int * string option * int * int) list }

  [<Procedure(Name = "ProcResultAndOut")>]
  type ProcEntityResultAndOut =
    { EmployeeId : int
      [<ProcedureParam(Direction = Direction.Result)>]
      EmpList : Employee list
      [<ProcedureParam(Direction = Direction.Output)>]
      EmployeeCount : int }

  [<Procedure(Name = "ProcResultAndOut")>]
  type ProcTupleResultAndOut =
    { EmployeeId : int
      [<ProcedureParam(Direction = Direction.Result)>]
      EmpList : (int * string option * int * int) list
      [<ProcedureParam(Direction = Direction.Output)>]
      EmployeeCount : int }

  type ProcResultAndUpdate =
    { EmployeeId : int
      [<ProcedureParam(Direction = Direction.Result)>]
      EmpList : Employee list }

  type ProcUpdateAndResult =
    { EmployeeId : int
      [<ProcedureParam(Direction = Direction.Result)>]
      EmpList : Employee list }

  [<Procedure(Name = "ProcResults")>]
  type ProcEntityResults =
    { EmployeeId : int
      DepartmentId : int
      [<ProcedureParam(Direction = Direction.Result)>]
      EmpList : Employee list
      [<ProcedureParam(Direction = Direction.Result)>]
      DeptList : Department list }

  [<Procedure(Name = "ProcResults")>]
  type ProcTupleResults =
    { EmployeeId : int
      DepartmentId : int
      [<ProcedureParam(Direction = Direction.Result)>]
      EmpList : (int * string * int * int) list
      [<ProcedureParam(Direction = Direction.Result)>]
      DeptList : (int * string * int) list }

  type FuncMultiParams =
    { Param1 : int
      Param2 : int
      [<ProcedureParam(Direction = Direction.ReturnValue)>]
      ReturnValue : int }

  [<Test>]
  let ``call : ProcNoneParam``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.call<ProcNoneParam> { Unit = () } }
    |> function
    | Success ret -> 
      ()
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``call : ProcSingleParam``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.call<ProcSingleParam> { Param1 = 10 } }
    |> function
    | Success ret -> 
      ret |> isEqualTo { Param1 = 10 }
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``call : PorcMultiParams``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.call<ProcMultiParams> { Param1 = 1; Hoge = 2; Param3 = 3 } }
    |> function
    | Success ret -> 
      ret |> isEqualTo { Param1 = 1; Hoge = 3; Param3 = 1}
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``call : ProcEntityResult``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.call<ProcEntityResult> { EmployeeId = 1; EmpList = [] } }
    |> function
    | Success ret -> 
      ret |> isEqualTo { 
        EmployeeId = 1
        EmpList = 
        [ { EmployeeId = Some 2; EmployeeName = Some "Smith"; DepartmentId = Some 1; VersionNo = Some 0 }
          { EmployeeId = Some 3; EmployeeName = Some "Jhon"; DepartmentId = Some 2; VersionNo = Some 0 }
          { EmployeeId = Some 4; EmployeeName = None; DepartmentId = None; VersionNo = Some 0 }] }
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``call : ProcTupleResult``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.call<ProcTupleResult> { EmployeeId = 1; EmpList = [] } }
    |> function
    | Success ret -> 
      ret |> isEqualTo { 
        EmployeeId = 1
        EmpList = 
        [ (2, Some "Smith", 1, 0)
          (3, Some "Jhon", 2, 0)
          (4, None, 0, 0 )] }
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``call : ProcEntityResultAndOut``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.call<ProcEntityResultAndOut> { EmployeeId = 1; EmpList = [] ; EmployeeCount = 0 } }
    |> function
    | Success ret -> 
      ret |> isEqualTo { 
        EmployeeId = 1
        EmployeeCount = 4
        EmpList = 
        [ { EmployeeId = Some 2; EmployeeName = Some "Smith"; DepartmentId = Some 1; VersionNo = Some 0 }
          { EmployeeId = Some 3; EmployeeName = Some "Jhon"; DepartmentId = Some 2; VersionNo = Some 0 }
          { EmployeeId = Some 4; EmployeeName = None; DepartmentId = None; VersionNo = Some 0 }]}
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``call : ProcTupleResultAndOut``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.call<ProcTupleResultAndOut> { EmployeeId = 1; EmpList = [] ; EmployeeCount = 0 } }
    |> function
    | Success ret -> 
      ret |> isEqualTo { 
        EmployeeId = 1
        EmployeeCount = 4
        EmpList = 
        [ (2, Some "Smith", 1, 0)
          (3, Some "Jhon", 2, 0)
          (4, None, 0, 0 )] }
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``call : ProcResultAndUpdate``() =
    Runner.rollbackOnly <| txSupports { 
      let! ret = Db.call<ProcResultAndUpdate> { EmployeeId = 1; EmpList = [] }
      let! dept = Db.find<Department> [1]
      return ret, dept }
    |> function
    | Success (ret, dept) -> 
      ret |> isEqualTo { 
        EmployeeId = 1
        EmpList = 
        [ { EmployeeId = Some 2; EmployeeName = Some "Smith"; DepartmentId = Some 1; VersionNo = Some 0 }
          { EmployeeId = Some 3; EmployeeName = Some "Jhon"; DepartmentId = Some 2; VersionNo = Some 0 }
          { EmployeeId = Some 4; EmployeeName = None; DepartmentId = None; VersionNo = Some 0 }]}
      dept |> isEqualTo { DepartmentId = 1; DepartmentName = "HOGE"; VersionNo = 0 }
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``call : ProcUpdateAndResult``() =
    Runner.rollbackOnly <| txSupports { 
      let! ret = Db.call<ProcUpdateAndResult> { EmployeeId = 1; EmpList = [] }
      let! dept = Db.find<Department> [1]
      return ret, dept }
    |> function
    | Success (ret, dept) -> 
      ret |> isEqualTo { 
        EmployeeId = 1
        EmpList = 
        [ { EmployeeId = Some 2; EmployeeName = Some "Smith"; DepartmentId = Some 1; VersionNo = Some 0 }
          { EmployeeId = Some 3; EmployeeName = Some "Jhon"; DepartmentId = Some 2; VersionNo = Some 0 }
          { EmployeeId = Some 4; EmployeeName = None; DepartmentId = None; VersionNo = Some 0 }]}
      dept |> isEqualTo { DepartmentId = 1; DepartmentName = "HOGE"; VersionNo = 0 }
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``call : ProcEntityResults``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.call<ProcEntityResults> { EmployeeId = 1; DepartmentId = 1; EmpList = [] ; DeptList = [] } }
    |> function
    | Success ret -> 
      ret.EmployeeId |> isEqualTo 1
      ret.DepartmentId |> isEqualTo 1
      List.length ret.EmpList |> isEqualTo 3
      List.length ret.DeptList |> isEqualTo 1
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``call : ProcTupleResults``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.call<ProcTupleResults> { EmployeeId = 1; DepartmentId = 1; EmpList = [] ; DeptList = [] } }
    |> function
    | Success ret -> 
      ret.EmployeeId |> isEqualTo 1
      ret.DepartmentId |> isEqualTo 1
      List.length ret.EmpList |> isEqualTo 3
      List.length ret.DeptList |> isEqualTo 1
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``call : FuncMultiParams``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.call<FuncMultiParams> { Param1 = 1; Param2 = 2; ReturnValue = 0 } }
    |> function
    | Success ret -> 
      ret |> isEqualTo { Param1 = 1; Param2 = 2; ReturnValue = 3 }
    | Failure e -> 
      raise <| Exception("", e)
