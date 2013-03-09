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

module FindTest = 

  type Department =
    { [<Id>]
      DepartmentId : int
      DepartmentName : string
      [<Version>]
      VersionNo : int }

  [<Table(IsEnclosed = true)>]
  type Employee =
    { [<Id(IdKind.Identity)>]
      EmployeeId : int option
      [<Column(IsEnclosed = true)>]
      EmployeeName : string option
      DepartmentId : int option
      [<Version>]
      VersionNo : int option }

  type Address =
    { [<Id(IdKind.Identity)>]
      AddressId : int
      Street : string
      [<Version>]
      VersionNo : byte array }

  type CompKeyEmployee =
    { [<Id>]
      EmployeeId1 : int
      [<Id>]
      EmployeeId2 : int
      EmployeeName : string
      [<Version>]
      VersionNo : int }

  type NoId =
    { Name : string
      VersionNo : int }

  type NoVersion =
    { [<Id>]
      Id : int
      Name : string }

  [<Test>]
  let ``find``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.find<Department> [2] }
    |> function
    | Success ret -> 
      ret |> isEqualTo { DepartmentId = 2; DepartmentName = "Sales"; VersionNo = 0 }
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``find : by composite id``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.find<CompKeyEmployee> [2; 12] }
    |> function
    | Success ret -> 
      ret |> isEqualTo { EmployeeId1 = 2; EmployeeId2 = 12; EmployeeName = "Smith"; VersionNo = 0 }
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``find : id type is option``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.find<Employee> [Some 1] }
    |> function
    | Success ret -> 
      ret |> isEqualTo { EmployeeId = Some 1; EmployeeName = Some "King"; DepartmentId = Some 1; VersionNo = Some 0 }
    | Failure e -> 
      raise <| Exception("", e)

  // TODO
  (*
  [<Test>]
  let ``find : id type is option but param type is not option``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.find<Employee> [1] }
    |> function
    | Success ret -> 
      ret |> isEqualTo { EmployeeId = Some 1; EmployeeName = Some "King"; DepartmentId = Some 1; VersionNo = Some 0 }
    | Failure e -> 
      raise <| Exception("", e)
  *)

  [<Test>]
  let ``find : not found``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.find<Department> [99] }
    |> function
    | Success ret -> 
      failwith "not expected"
    | Failure e -> 
      // TODO
      messageId e |> isEqualTo "TRANQ4015"

  [<Test>]
  let ``find : by empty id``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.find<NoId> [] }
    |> function
    | Success _ -> 
      failwith "not expected"
    | Failure e -> 
      messageId e |> isEqualTo "TRANQ4004"

  [<Test>]
  let ``find : by id for no id record``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.find<NoId> [2] }
    |> function
    | Success _ -> 
      failwith "not expected"
    | Failure e ->
      messageId e |> isEqualTo "TRANQ4005"

  [<Test>]
  let ``tryFind : id``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.tryFind<Department> [2] }
    |> function
    | Success ret -> 
      ret |> isEqualTo (Some { DepartmentId = 2; DepartmentName = "Sales"; VersionNo = 0 })
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``tryFind : by composite id``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.tryFind<CompKeyEmployee> [2; 12] }
    |> function
    | Success ret -> 
      ret |> isEqualTo (Some { EmployeeId1 = 2; EmployeeId2 = 12; EmployeeName = "Smith"; VersionNo = 0 })
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``tryFind : not found``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.tryFind<Department> [99] }
    |> function
    | Success ret -> 
      ret |> isEqualTo None
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``findWithVersion``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.findWithVersion<Department> [2] 0 }
    |> function
    | Success ret -> 
      ret |> isEqualTo { DepartmentId = 2; DepartmentName = "Sales"; VersionNo = 0; }
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``findWithVersion : by composite id``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.findWithVersion<CompKeyEmployee> [2; 12] 0 }
    |> function
    | Success ret -> 
      ret |> isEqualTo { EmployeeId1 = 2; EmployeeId2 = 12; EmployeeName = "Smith"; VersionNo = 0; }
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``findWithVersion : not found``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.findWithVersion<Department> [99] 0 }
    |> function
    | Success ret -> 
      failwith "not expected"
    | Failure e -> 
      // TODO
      messageId e |> isEqualTo "TRANQ4015"

  [<Test>]
  let ``findWithVersion : optimistic lock confliction``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.findWithVersion<Department> [2] 99 }
    |> function
    | Success ret -> 
      failwith "not expected"
    | Failure e -> 
      match e with 
      | OptimisticLockError _ -> ()
      | _ -> raise <| Exception("", e)

  [<Test>]
  let ``tryFindWithVersion``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.tryFindWithVersion<Department> [2] 0 }
    |> function
    | Success ret -> 
      ret |> isEqualTo (Some { DepartmentId = 2; DepartmentName = "Sales"; VersionNo = 0; }) 
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``tryFindWithVersion : by composite id``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.tryFindWithVersion<CompKeyEmployee> [2; 12] 0 }
    |> function
    | Success ret -> 
      ret |> isEqualTo (Some { EmployeeId1 = 2; EmployeeId2 = 12; EmployeeName = "Smith"; VersionNo = 0; })
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``tryFindWithVersion : not found``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.tryFindWithVersion<Department> [99] 0 }
    |> function
    | Success ret -> 
      ret |> isEqualTo None
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``tryFindWithVersion : optimistic lock confliction``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.tryFindWithVersion<Department> [2] 99 }
    |> function
    | Success ret -> 
      ret |> isEqualTo None
    | Failure e -> 
      match e with 
      | OptimisticLockError _ -> ()
      | _ -> raise <| Exception("", e)
