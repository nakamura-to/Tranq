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

module IterateTest = 

  type Employee =
    { [<Id(IdKind.Identity)>]
      EmployeeId : int option
      EmployeeName : string option
      DepartmentId : int option
      [<Version>]
      VersionNo : int option }

  [<Test>]
  let ``iterate``() =
    let array = ResizeArray()
    Runner.rollbackOnly <| txRequired { 
      return! Db.iterate<Employee> "
        select * from Employee order by EmployeeId
        " 
        <| [] 
        <| Range(Offset = 1L, Limit = 2L) 
        <| fun employee -> array.Add(employee); true } 
    |> function
    | Success ret -> 
      array.Count |> isEqualTo 2
      array.[0] |> isEqualTo { EmployeeId = Some 2; EmployeeName = Some "Smith"; DepartmentId = Some 1; VersionNo = Some 0 }
      array.[1] |> isEqualTo { EmployeeId = Some 3; EmployeeName = Some "Jhon"; DepartmentId = Some 2; VersionNo = Some 0 }
    | Failure e -> 
      raise <| Exception("", e)

  [<Test>]
  let ``iterate : break``() =
    let array = ResizeArray()
    Runner.rollbackOnly <| txRequired { 
      return! Db.iterate<Employee> "
        select * from Employee order by EmployeeId
        " 
        <| [] 
        <| Range(Offset = 1L, Limit = 2L) 
        <| fun employee -> array.Add(employee); array.Count < 1 } 
    |> function
    | Success ret -> 
      array.Count |> isEqualTo 1
      array.[0] |> isEqualTo { EmployeeId = Some 2; EmployeeName = Some "Smith"; DepartmentId = Some 1; VersionNo = Some 0 }
    | Failure e -> 
      raise <| Exception("", e)

