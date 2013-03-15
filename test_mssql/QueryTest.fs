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

module QueryTest = 

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

  type Duplication =
    { aaa : int
      aaa1 : int
      bbb : string }

  [<Test>]
  let ``query : 1 record``() =
    Runner.rollbackOnly <| txRequired { 
      return! Db.query<Department> "
        select * from Department where DepartmentId = /* id */0
        " ["id" <-- 2] }
    |> function
    | Success ret -> 
      List.length ret |> isEqualTo 1
      List.head ret |> isEqualTo { DepartmentId = 2; DepartmentName = "Sales"; VersionNo = 0 }
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``query : 1 record by Enum value``() =
    Runner.rollbackOnly <| txRequired { 
      return! Db.query<Person> "
        select * from Person where JobKind = /* jobKind */0
        " ["jobKind" <-- JobKind.Manager] }
    |> function
    | Success ret -> 
      List.length ret |> isEqualTo 1
      List.head ret |> isEqualTo { PersonId = 2; PersonName = "Martin"; JobKind = JobKind.Manager; VersionNo = 0 }
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``query : all records``() =
    Runner.rollbackOnly <| txRequired { 
      return! Db.query<Department> "
        select * from Department order by DepartmentId
        " [] }
    |> function
    | Success ret -> 
      List.length ret |> isEqualTo 2
      ret |> isEqualTo [
        { DepartmentId = 1; DepartmentName = "Account"; VersionNo = 0 }
        { DepartmentId = 2; DepartmentName = "Sales"; VersionNo = 0 }]
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``query : 1 record: option``() =
    Runner.rollbackOnly <| txRequired { 
      return! Db.query<Employee> "
        select * from Employee where EmployeeId = 4
        " [] }
    |> function
    | Success ret -> 
      List.length ret |> isEqualTo 1
      List.head ret |> isEqualTo { EmployeeId = Some 4; EmployeeName = None; DepartmentId = None; VersionNo = Some 0 }
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``query : using if expression comment``() =
    let greaterThanZero id = id > 1
    Runner.rollbackOnly <| txRequired { 
      return! Db.query<Department> "
        select * from Department where 
        /*% if greaterThanZero id */ 
          DepartmentId = /* id */0 
        /*% end */
        " ["id" <-- 2; "greaterThanZero" <-- greaterThanZero] }
    |> function
    | Success ret -> 
      List.length ret |> isEqualTo 1
      List.head ret |> isEqualTo { DepartmentId = 2; DepartmentName = "Sales"; VersionNo = 0 }
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``query : using in operation``() =
    Runner.rollbackOnly <| txRequired { 
      return! Db.query<Department> "
        select * from Department where DepartmentId in /* id */(10, 20)
        " ["id" <-- [1; 2]] }
    |> function
    | Success ret -> 
      List.length ret |> isEqualTo 2
      ret |> isEqualTo [
        { DepartmentId = 1; DepartmentName = "Account"; VersionNo = 0 }
        { DepartmentId = 2; DepartmentName = "Sales"; VersionNo = 0 }] 
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``query : 1 tuple``() =
    Runner.rollbackOnly <| txRequired { 
      return! Db.query<int * string * int> "
        select DepartmentId, DepartmentName, VersionNo from Department where DepartmentId = /* id */0
        " ["id" <-- 2] }
    |> function
    | Success ret -> 
      List.length ret |> isEqualTo 1
      List.head ret |> isEqualTo (2, "Sales", 0)
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``query : all tuples``() =
    Runner.rollbackOnly <| txRequired { 
      return! Db.query<int * string * int> "
        select * from Department
        " ["id" <-- 2] }
    |> function
    | Success ret -> 
      List.length ret |> isEqualTo 2
      ret |> isEqualTo [(1, "Account", 0); (2, "Sales", 0)]
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``query : 1 tuple : record mixed``() =
    Runner.rollbackOnly <| txRequired { 
      return! Db.query<string * Employee> "
        select d.DepartmentName, e.* from Department d inner join Employee e on (d.DepartmentId = e.DepartmentId) where d.DepartmentId = /* id */0
        " ["id" <-- 2] }
    |> function
    | Success ret -> 
      List.length ret |> isEqualTo 1
      List.head ret |> isEqualTo (
        "Sales", { EmployeeId = Some 3; EmployeeName = Some "Jhon"; DepartmentId = Some 2; VersionNo = Some 0 })
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``query : 1 single``() =
    Runner.rollbackOnly <| txRequired { 
      return! Db.query<string> "
        select DepartmentName from Department where DepartmentId = /* id */0
        " ["id" <-- 2] }
    |> function
    | Success ret -> 
      List.length ret |> isEqualTo 1
      List.head ret |> isEqualTo "Sales"
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``query : column not found``() =
    Runner.rollbackOnly <| txRequired { 
      return! Db.query<Department> "
        select DepartmentName from Department where DepartmentId = /* id */0
        " ["id" <-- 2] }
    |> function
    | Success ret -> 
      List.length ret |> isEqualTo 1
      List.head ret |> isEqualTo { DepartmentId = 0; DepartmentName = "Sales"; VersionNo = 0 }
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``query : 1 record : column duplicated``() =
    Runner.rollbackOnly <| txRequired { 
      return! Db.query<Duplication> "
        select DepartmentId aaa, DepartmentName bbb, VersionNo aaa from Department where DepartmentId = /* id */0
        " ["id" <-- 2] }
    |> function
    | Success ret -> 
      List.length ret |> isEqualTo 1
      List.head ret |> isEqualTo { aaa = 2; aaa1 = 0; bbb = "Sales" }
    | Failure e -> 
      raise <| Exception("", e)