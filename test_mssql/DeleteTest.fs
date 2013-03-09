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
open System.Configuration
open NUnit.Framework
open Tranq

module DeleteTest = 

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

  type NoId =
    { Name : string
      VersionNo : int }

  type NoVersion =
    { [<Id>]
      Id : int
      Name : string }

  [<Test>]
  let ``delete : no id``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.delete { NoId.Name = "aaa"; VersionNo = 0 } }
    |> function
    | Success ret -> 
      failwith "not expected"
    | Failure e -> 
      messageId e |> isEqualTo "TRANQ4005"

  [<Test>]
  let ``delete : no version``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.delete { NoVersion.Id = 1; Name = "aaa" } }
    |> function
    | Success ret -> 
      ()
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``delete : incremented version``() =
    Runner.rollbackOnly <| txSupports { 
      let! department = Db.find<Department> [1]
      return! Db.delete department }
    |> function
    | Success ret -> 
      ()
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``delete : computed version``() =
    Runner.rollbackOnly <| txSupports { 
      let! address = Db.insert { AddressId = 0; Street = "hoge"; VersionNo = Array.empty }
      return! Db.delete address }
    |> function
    | Success ret -> 
      ()
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``delete : incremented version : optimistic lock confliction``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.delete { DepartmentId = 1; DepartmentName = "hoge"; VersionNo = -1 } }
    |> function
    | Success ret -> 
      failwith "not expected"
    | Failure e -> 
      match e with 
      | OptimisticLockError _ -> ()
      | _ -> raise <| Exception("", e)

  [<Test>]
  let ``delete : computed version : optimistic lock confliction``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.delete { AddressId = 1; Street = "hoge"; VersionNo = Array.empty } }
    |> function
    | Success ret -> 
      failwith "not expected"
    | Failure e -> 
      match e with 
      | OptimisticLockError _ -> ()
      | _ -> raise <| Exception("", e)

  [<Test>]
  let ``deleteIgnoreVersion``() =
    Runner.rollbackOnly <| txSupports { 
      let opt = DeleteOpt(IgnoreVersion = true)
      return! Db.deleteWithOpt { DepartmentId = 1; DepartmentName = "aaa"; VersionNo = -1 } opt }
    |> function
    | Success ret -> 
      ()
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``deleteIgnoreVersion : no affected row``() =
    Runner.rollbackOnly <| txSupports { 
      let opt = DeleteOpt(IgnoreVersion = true)
      return! Db.deleteWithOpt { DepartmentId = 0; DepartmentName = "aaa"; VersionNo = -1 } opt }
    |> function
    | Success ret -> 
      failwith "not expected"
    | Failure e -> 
      messageId e |> isEqualTo "TRANQ4011"
