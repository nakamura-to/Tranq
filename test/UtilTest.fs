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

namespace Tranq.Test

open Tranq
open NUnit.Framework
open TestTool

module OptionTest =

  [<Test>]
  let ``asOption`` () =
    assert_equal (Some "aaa") (Option.asOption "aaa")
    assert_equal (None) (Option.asOption null)

  [<Test>]
  let ``isOptionType`` () =
    assert_true (Option.isOptionType typeof<int option>)
    assert_true (Option.isOptionType typeof<string option>)
    assert_false (Option.isOptionType typeof<int>)
    assert_false (Option.isOptionType typeof<string>)

  [<Test>]
  let ``make`` () =
    assert_equal (Some 1) (Option.make typeof<int option> 1)
    assert_equal (Some "1") (Option.make typeof<string option> 1)
    assert_equal None (Option.make typeof<int option> null)
    assert_equal None (Option.make typeof<string option> null)

  [<Test>]
  let ``getOptionElement`` () =
    assert_equal (box 1, typeof<int>) (Option.getElement typeof<int option> (Some 1))
    assert_equal (box "a", typeof<string>) (Option.getElement typeof<string option> (Some "a"))
    assert_equal (null, typeof<int>) (Option.getElement typeof<int option> None)
    assert_equal (null, typeof<string>) (Option.getElement typeof<string option> None)

module DictTest =

  [<Test>]
  let ``toList`` () =
    let list = Dict.toList (dict [1, "A"; 2, "B"])
    assert_equal [1, "A"; 2, "B"] list

  [<Test>]
  let ``toSeq`` () =
    let seq = Dict.toSeq (dict [1, "A"; 2, "B"])
    assert_equal [1, "A"; 2, "B"] (Seq.toList seq)

  [<Test>]
  let ``toArray`` () =
    let array = Dict.toArray (dict [1, "A"; 2, "B"])
    assert_equal [|1, "A"; 2, "B"|] array

module MapTest =

  [<Test>]
  let ``ofDict`` () =
    let map = Map.ofDict (dict [1, "A"; 2, "B"])
    assert_equal [1, "A"; 2, "B"] (Map.toList map)

module SeqTest =

  [<Test>]
  let ``peek`` () =
    let seq = seq {1..3}
    let result = Seq.peek seq
    assert_equal [1, true; 2, true; 3, false] (Seq.toList result)

  [<Test>]
  let ``changeTypeFromSeqToList`` () =
    let seq:seq<obj> = seq { yield box 1; yield box 2 }
    let list = Seq.changeToList typeof<int> seq
    match list with
    | :? (list<int>) ->()
    | _ -> fail ()

  [<Test>]
  let ``changeTypeFromSeqToResizeArray`` () =
    let seq:seq<obj> = seq { yield box 1; yield box 2 }
    let list = Seq.changeToResizeArray typeof<int> seq
    match list with
    | :? (ResizeArray<int>) ->()
    | _ -> fail ()

module NumberTest =
  open System

  [<Test>]
  let ``isNumberType`` () =
    assert_true (Number.isNumberType typeof<int>)
    assert_false (Number.isNumberType typeof<string>)

  [<Test>]
  let ``one`` () =
    assert_equal 1 (Number.one typeof<int>)
    assert_equal 1L (Number.one typeof<int64>)
    assert_equal 1M (Number.one typeof<decimal>)

  [<Test>]
  let ``incr`` () =
    assert_equal 2 (Number.incr 1)
    assert_equal 2L (Number.incr 1L)
    assert_equal 2M (Number.incr 1M)

  [<Test>]
  let ``lessThan`` () =
    assert_true (Number.lessThan (1, 2))
    assert_false (Number.lessThan (1, 1))
    assert_false (Number.lessThan (1, 0))
    assert_true (Number.lessThan (1L, 2))
    assert_false (Number.lessThan (1L, 1))
    assert_false (Number.lessThan (1L, 0))

module BasicTest =
  open System

  [<Test>]
  let ``isBasicType`` () =
    assert_true (Basic.isBasicType typeof<int>)
    assert_true (Basic.isBasicType typeof<int option>)
    assert_true (Basic.isBasicType typeof<DayOfWeek>)
    assert_true (Basic.isBasicType typeof<DayOfWeek option>)
    assert_false (Basic.isBasicType typeof<obj>)
