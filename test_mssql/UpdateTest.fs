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

module UpdateTest = 

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

  [<Test>]
  let ``update : no id``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.update { NoId.Name = "aaa"; VersionNo = 0 } }
    |> function
    | Success ret -> 
      failwith "not expected"
    | Failure e -> 
      messageId e |> isEqualTo "TRANQ4005"

  [<Test>]
  let ``update : composite id``() =
    Runner.rollbackOnly <| txSupports { 
      let! employee = Db.find<CompKeyEmployee> [2; 12]
      return! Db.update { employee with EmployeeName = "hoge" } }
    |> function
    | Success ret -> 
      ret |> isEqualTo { EmployeeId1 = 2; EmployeeId2 = 12; EmployeeName = "hoge"; VersionNo = 1; }
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``update : no version``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.update { NoVersion.Id = 1; Name = "aaa" } }
    |> function
    | Success ret -> 
      ret |> isEqualTo { NoVersion.Id = 1; Name = "aaa" }
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``update : incremented version``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.update { DepartmentId = 1; DepartmentName = "hoge"; VersionNo = 0 } }
    |> function
    | Success ret -> 
      ret |> isEqualTo { DepartmentId = 1; DepartmentName = "hoge"; VersionNo = 1 }
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``update : computed version``() =
    let version = ref ([||] : byte array)
    Runner.rollbackOnly <| txSupports { 
      let! address = Db.insert { Address.AddressId = 0; Street = "hoge"; VersionNo = Array.empty }
      let address = { address with Street = "foo" }
      version := address.VersionNo
      return! Db.update address }
    |> function
    | Success { VersionNo = ver } -> 
      ver |> isNotEqualTo !version
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``update : Enum``() =
    Runner.rollbackOnly <| txSupports { 
      let! person = Db.find<Person> [2]
      return! Db.update { person with JobKind = JobKind.Salesman } }
    |> function
    | Success ret -> 
      ret |> isEqualTo { PersonId = 2; PersonName = "Martin"; JobKind = JobKind.Salesman; VersionNo = 1 }
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``update : unique constraint violation``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.update { DepartmentId = 1; DepartmentName = "Sales"; VersionNo = 0 } }
    |> function
    | Success ret -> 
      failwith "not expected"
    | Failure e -> 
      match e with 
      | UniqueConstraintError _ -> ()
      | _ -> raise <| Exception("", e)

  [<Test>]
  let ``update : optimistic lock confliction``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.update { DepartmentId = 1; DepartmentName = "hoge"; VersionNo = -1 } }
    |> function
    | Success ret -> 
      failwith "not expected"
    | Failure e -> 
      match e with 
      | OptimisticLockError _ -> ()
      | _ -> raise <| Exception("", e)

  [<Test>]
  let ``updateIgnoreVersion``() =
    Runner.rollbackOnly <| txSupports { 
      let opt = UpdateOpt(IgnoreVersion = true) 
      return! Db.updateWithOpt { DepartmentId = 1; DepartmentName = "hoge"; VersionNo = -1 } opt }
    |> function
    | Success ret -> 
      ()
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``updateIgnoreVersion : no affected row``() =
    Runner.rollbackOnly <| txSupports { 
      let opt = UpdateOpt(IgnoreVersion = true) 
      return! Db.updateWithOpt { DepartmentId = 0; DepartmentName = "hoge"; VersionNo = -1 } opt }
    |> function
    | Success ret -> 
      failwith "not expected"
    | Failure e -> 
      messageId e |> isEqualTo "TRANQ4011"

  (* TODO
  [<Test>]
  let ``update : exclude none``() =
    Runner.rollbackOnly <| txSupports { 
      let! employee = Db.find<Employee> [1]
      let opt = UpdateOpt(ExcludeNone = true) 
      return! Db.updateWithOpt { employee with EmployeeName = None} opt }
    |> function
    | Success ret -> 
      ()
    | Failure e -> 
      raise <| Exception("", e)
  *)

  [<Test>]
  let ``update : exclude none``() =
    Runner.rollbackOnly <| txSupports { 
      let! employee = Db.find<Employee> [Some 1]
      let opt = UpdateOpt(ExcludeNone = true) 
      do! Db.updateWithOpt { employee with EmployeeName = None} opt |> Tx.ignore
      return! Db.find<Employee> [Some 1] }
    |> function
    | Success { EmployeeName = name } -> 
      name |> isNotEqualTo None
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``update : exclude``() =
    Runner.rollbackOnly <| txSupports { 
      let! employee = Db.find<Employee> [Some 1]
      let opt = UpdateOpt(Exclude = ["EmployeeName"]) 
      do! Db.updateWithOpt { employee with EmployeeName = Some "hoge"; DepartmentId = Some 2} opt |> Tx.ignore
      return! Db.find<Employee> [Some 1] }
    |> function
    | Success { EmployeeName = name; DepartmentId = deptId } -> 
      name |> isNotEqualTo (Some "hoge")
      deptId |> isEqualTo (Some 2)
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``update : include``() =
    Runner.rollbackOnly <| txSupports { 
      let! employee = Db.find<Employee> [Some 1]
      let opt = UpdateOpt(Include = ["EmployeeName"]) 
      do! Db.updateWithOpt { employee with EmployeeName = Some "hoge"; DepartmentId = Some 2} opt |> Tx.ignore
      return! Db.find<Employee> [Some 1] }
    |> function
    | Success { EmployeeName = name; DepartmentId = deptId } -> 
      name |> isEqualTo (Some "hoge")
      deptId |> isNotEqualTo (Some 2)
    | Failure e -> 
      raise <| Exception("", e)
