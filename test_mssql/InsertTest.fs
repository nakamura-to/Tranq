//----------------------------------------------------------------------------
//
// Copyright (c) 2011 The Soma Team. 
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
open System.Configuration
open NUnit.Framework
open Tranq

module InsertTest = 

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

  type Address =
    { [<Id(IdKind.Identity)>]
      AddressId : int
      Street : string
      [<Version(VersionKind.Computed)>]
      VersionNo : byte array }

  type JobKind =
    | Salesman = 0
    | Manager = 1

  type Person =
    { [<Id(IdKind.Identity)>]
      PersonId : int
      PersonName : string
      JobKind : JobKind
      [<Version>]
      VersionNo : int }

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
  let ``insert : assigned id``() =
    Runner.rollbackOnly <| txSupports { 
      let department = { DepartmentId = 99; DepartmentName = "aaa"; VersionNo = 0 }
      return! Db.insert department }
    |> function
    | Success ret -> 
      ret |> isEqualTo { DepartmentId = 99; DepartmentName = "aaa"; VersionNo = 1 }
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``insert : assigned composite id``() =
    Runner.rollbackOnly <| txSupports { 
      let employee = { EmployeeId1 = 99; EmployeeId2 = 1; EmployeeName = "aaa"; VersionNo = 0 }
      return! Db.insert employee }
    |> function
    | Success ret -> 
      ret |> isEqualTo { EmployeeId1 = 99; EmployeeId2 = 1; EmployeeName = "aaa"; VersionNo = 1 }
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``insert : identity id``() =
    Runner.rollbackOnly <| txSupports { 
      let employee = { Employee.EmployeeId = None; EmployeeName = Some "hoge"; DepartmentId = Some 1; VersionNo = Some 0 }
      return! Db.insert employee }
    |> function
    | Success { EmployeeId = id; VersionNo = ver } -> 
      Option.isSome id |> isEqualTo true
      ver |> isEqualTo (Some 1)
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``insert : no id``() =
    Runner.rollbackOnly <| txSupports { 
      let noId = { NoId.Name = "aaa"; VersionNo = 0 }
      return! Db.insert noId }
    |> function
    | Success ret -> 
      ret |> isEqualTo { NoId.Name = "aaa"; VersionNo = 0 }
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``insert : no version``() =
    Runner.rollbackOnly <| txSupports { 
      let noVersion = { NoVersion.Id = 99; Name = "aaa" }
      return! Db.insert noVersion }
    |> function
    | Success ret -> 
      ret |> isEqualTo { NoVersion.Id = 99; Name = "aaa" }
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``insert : unique constraint violation : unique key``() =
    Runner.rollbackOnly <| txSupports { 
      let department = { DepartmentId = 1; DepartmentName = "aaa"; VersionNo = 0 }
      return! Db.insert department }
    |> function
    | Success ret -> 
      failwith "not expected"
    | Failure e -> 
      match e with 
      | UniqueConstraintError _ -> ()
      | _ -> raise <| Exception("", e)

  [<Test>]
  let ``insert : unique constraint violation : unique index``() =
    Runner.rollbackOnly <| txSupports { 
      let person = { PersonId = 0; PersonName = "Scott"; JobKind = JobKind.Salesman; VersionNo = 0 }
      return! Db.insert person }
    |> function
    | Success ret -> 
      failwith "not expected"
    | Failure e -> 
      match e with 
      | UniqueConstraintError _ -> ()
      | _ -> raise <| Exception("", e)

  [<Test>]
  let ``insert : incremented version``() =
    Runner.rollbackOnly <| txSupports { 
      let employee = { Employee.EmployeeId = None; EmployeeName = None; DepartmentId = Some 1; VersionNo = None }
      return! Db.insert employee }
    |> function
    | Success { EmployeeId = id; VersionNo = ver} -> 
      Option.isSome id |> isEqualTo true
      ver |> isEqualTo (Some 1)
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``insert : computed version``() =
    Runner.rollbackOnly <| txSupports { 
      let address = { Address.AddressId = 0; Street = "hoge"; VersionNo = Array.empty }
      return! Db.insert address }
    |> function
    | Success { VersionNo = ver} -> 
      (Array.isEmpty ver) |> isFalse
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``insert : exclude none``() =
    Runner.rollbackOnly <| txSupports { 
      let employee = { Employee.EmployeeId = None; EmployeeName = None; DepartmentId = None; VersionNo = Some 0 }
      return! Db.insertWithOpt employee (InsertOpt(ExcludeNone = true)) }
    |> function
    | Success { EmployeeId = id} -> 
      Option.isSome id |> isTrue
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``insert : exclude``() =
    Runner.rollbackOnly <| txSupports { 
      let employee = { Employee.EmployeeId = None; EmployeeName = Some "hoge"; DepartmentId = Some 1; VersionNo = Some 0 }
      return! Db.insertWithOpt employee (InsertOpt(Exclude = ["EmployeeName"])) }
    |> function
    | Success { VersionNo = ver } -> 
      ver |> isEqualTo (Some 1)
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``insert : include``() =
    Runner.rollbackOnly <| txSupports { 
      let employee = { Employee.EmployeeId = None; EmployeeName = Some "hoge"; DepartmentId = Some 1; VersionNo = Some 0 }
      return! Db.insertWithOpt employee (InsertOpt(Include = ["EmployeeName"])) }
    |> function
    | Success { VersionNo = ver } -> 
      ver |> isEqualTo (Some 1)
    | Failure e -> 
      raise <| Exception("", e)

