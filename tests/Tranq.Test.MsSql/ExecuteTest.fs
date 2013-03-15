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
open System.Data
open NUnit.Framework
open Tranq

module ExecuteTest = 

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
      [<Version>]
      VersionNo : byte array }

  [<Test>]
  let ``execute : insert``() =
    Runner.rollbackOnly <| txRequired { 
      return! Db.execute "
        insert into Department (DepartmentId, DepartmentName, VersionNo) values (/* id */0, /* name */'aaa', /* version */0)
        " ["id" <-- 99; "name" <-- "hoge"; "version" <-- 10] }
    |> function
    | Success ret -> 
      ret |> isEqualTo 1
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``execute : update``() =
    Runner.rollbackOnly <| txRequired { 
      return! Db.execute "
        update Department set DepartmentName = N'hoge' where DepartmentId = /* id */-1 and VersionNo = /* versionNo */-1
        " ["id" <-- 1; "versionNo" <-- 0] }
    |> function
    | Success ret -> 
      ret |> isEqualTo 1
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``execute : delete``() =
    Runner.rollbackOnly <| txRequired { 
      return! Db.execute "
        delete from Department
        " [] }
    |> function
    | Success ret -> 
      ret |> isEqualTo 2
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``executeReader``() =
    Runner.rollbackOnly <| txRequired { 
      return! Db.executeReader "
        select * from Department where DepartmentId = /* id */0
        " ["id" <-- 2] <| fun reader ->
        let table = new DataTable()
        table.Load reader
        table }
    |> function
    | Success ret -> 
      ret.Rows.Count |> isEqualTo 1
      ret.Rows.[0].["DepartmentName"] |> string |> isEqualTo "Sales"
    | Failure e -> 
      raise <| Exception("", e)
