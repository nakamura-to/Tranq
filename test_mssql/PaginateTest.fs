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

module PaginateTest = 

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
  let ``paginate``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.paginate<Employee> "
        select * from Employee order by EmployeeId
        " [] <| PaginateOpt(Offset = 1L, Limit = 2L) }
    |> function
    | Success ret -> 
      List.length ret |> isEqualTo 2
      ret |> isEqualTo [
        { EmployeeId = Some 2; EmployeeName = Some "Smith"; DepartmentId = Some 1; VersionNo = Some 0}
        { EmployeeId = Some 3; EmployeeName = Some "Jhon"; DepartmentId = Some 2; VersionNo = Some 0}]
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``paginate : offset is positive``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.paginate<Employee> "
        select * from Employee order by EmployeeId
        " [] <| PaginateOpt(Offset = 1L, Limit = 2L) }
    |> function
    | Success ret -> 
      List.length ret |> isEqualTo 2
      ret |> isEqualTo [
        { EmployeeId = Some 2; EmployeeName = Some "Smith"; DepartmentId = Some 1; VersionNo = Some 0}
        { EmployeeId = Some 3; EmployeeName = Some "Jhon"; DepartmentId = Some 2; VersionNo = Some 0}]
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``paginate : offset is positive : embedded variable in order by clause``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.paginate<Employee> "
         select * from Employee E order by /*#orderby*/
        " ["orderby" <-- "E.EmployeeId"] <| PaginateOpt(Offset = 1L, Limit = 2L) }
    |> function
    | Success ret -> 
      List.length ret |> isEqualTo 2
      ret |> isEqualTo [
        { EmployeeId = Some 2; EmployeeName = Some "Smith"; DepartmentId = Some 1; VersionNo = Some 0}
        { EmployeeId = Some 3; EmployeeName = Some "Jhon"; DepartmentId = Some 2; VersionNo = Some 0}]
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``paginate : offset is zero``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.paginate<Employee> "
         select * from Employee order by EmployeeId
        " [] <| PaginateOpt(Offset = 0L, Limit = 2L) }
    |> function
    | Success ret -> 
      List.length ret |> isEqualTo 2
      ret |> isEqualTo [
        { EmployeeId = Some 1; EmployeeName = Some "King"; DepartmentId = Some 1; VersionNo = Some 0 }
        { EmployeeId = Some 2; EmployeeName = Some "Smith"; DepartmentId = Some 1; VersionNo = Some 0 }]
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``paginate : offset is negative``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.paginate<Employee> "
         select * from Employee order by EmployeeId
        " [] <| PaginateOpt(Offset = -1L, Limit = 2L) }
    |> function
    | Success ret -> 
      List.length ret |> isEqualTo 2
      ret |> isEqualTo [
        { EmployeeId = Some 1; EmployeeName = Some "King"; DepartmentId = Some 1; VersionNo = Some 0 }
        { EmployeeId = Some 2; EmployeeName = Some "Smith"; DepartmentId = Some 1; VersionNo = Some 0 }]
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``paginate : limit is positive``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.paginate<Employee> "
         select * from Employee order by EmployeeId
        " [] <| PaginateOpt(Offset = 1L, Limit = 1L) }
    |> function
    | Success ret -> 
      List.length ret |> isEqualTo 1
      ret |> isEqualTo [
        { EmployeeId = Some 2; EmployeeName = Some "Smith"; DepartmentId = Some 1; VersionNo = Some 0 }]
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``paginate : limit is zero``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.paginate<Employee> "
         select * from Employee order by EmployeeId
        " [] <| PaginateOpt(Offset = 1L, Limit = 0L) }
    |> function
    | Success ret -> 
      List.length ret |> isEqualTo 0
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``paginate : limit is negative``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.paginate<Employee> "
         select * from Employee order by EmployeeId
        " [] <| PaginateOpt(Offset = 1L, Limit = -1L) }
    |> function
    | Success ret -> 
      List.length ret |> isEqualTo 3
      ret |> isEqualTo [
        { EmployeeId = Some 2; EmployeeName = Some "Smith"; DepartmentId = Some 1; VersionNo = Some 0 }
        { EmployeeId = Some 3; EmployeeName = Some "Jhon"; DepartmentId = Some 2; VersionNo = Some 0 }
        { EmployeeId = Some 4; EmployeeName = None; DepartmentId = None; VersionNo = Some 0 }]
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``paginateAndCount``() =
    Runner.rollbackOnly <| txSupports { 
      return! Db.paginateAndCount<Employee> "
         select * from Employee order by EmployeeId
        " [] <| PaginateOpt(Offset = 1L, Limit = 2L) }
    |> function
    | Success (list, count) -> 
      List.length list |> isEqualTo 2
      list |> isEqualTo [
        { EmployeeId = Some 2; EmployeeName = Some "Smith"; DepartmentId = Some 1; VersionNo = Some 0 }
        { EmployeeId = Some 3; EmployeeName = Some "Jhon"; DepartmentId = Some 2; VersionNo = Some 0 }]
      count |> isEqualTo 4L
    | Failure e -> 
      raise <| Exception("", e)
