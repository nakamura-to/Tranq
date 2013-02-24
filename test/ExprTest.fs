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

module ExprTest =
  open System
  open System.Collections
  open System.Collections.Generic
  open Microsoft.FSharp.Text.Lexing
  open Microsoft.FSharp.Quotations
  open Microsoft.FSharp.Reflection
  open NUnit.Framework
  open Tranq
  open Tranq.Text
  open Tranq.ExprAst
  open TestTool

  type Hoge = { Name : string }

  type レコード = { 名前 : string }

  type レコード2 = { ``名前[]`` : string }

  [<Test>]
  let ``parse Unit`` () =
    match Expr.parse "()" with
    | Factor(Unit) -> ()
    | x -> fail x

  [<Test>]
  let ``parse Null`` () =
    match Expr.parse "null" with
    | Factor(Null) -> ()
    | x -> fail x

  [<Test>]
  let ``parse Boolean : true`` () =
    match Expr.parse "true" with
    | Factor(Boolean true) -> ()
    | x -> fail x

  [<Test>]
  let ``parse Boolean : false`` () =
    match Expr.parse "false" with
    | Factor(Boolean false) -> ()
    | x -> fail x

  [<Test>]
  let ``parse Byte`` () =
    match Expr.parse "1uy" with
    | Factor(Byte 1uy) -> ()
    | x -> fail x

  [<Test>]
  let ``parse Byte : fail`` () =
    try
      Expr.parse "1000uy" |> ignore
      fail ()
    with
    | :? ExprException as ex -> 
      printfn "%s" ex.Message
      assert_equal "TRANQ1000" ex.MessageId
    | ex -> 
      fail ex

  [<Test>]
  let ``parse SByte`` () =
    match Expr.parse "1y" with
    | Factor(SByte 1y) -> ()
    | x -> fail x

  [<Test>]
  let ``parse Int16`` () =
    match Expr.parse "1s" with
    | Factor(Int16 1s) -> ()
    | x -> fail x

  [<Test>]
  let ``parse UInt16`` () =
    match Expr.parse "1us" with
    | Factor(UInt16 1us) -> ()
    | x -> fail x

  [<Test>]
  let ``parse Int32`` () =
    match Expr.parse "1" with
    | Factor(Int32 1) -> ()
    | x -> fail x

  [<Test>]
  let ``parse UInt32`` () =
    match Expr.parse "1u" with
    | Factor(UInt32 1u) -> ()
    | x -> fail x

  [<Test>]
  let ``parse Int64`` () =
    match Expr.parse "1L" with
    | Factor(Int64 1L) -> ()
    | x -> fail x

  [<Test>]
  let ``parse UInt64`` () =
    match Expr.parse "1UL" with
    | Factor(UInt64 1UL) -> ()
    | x -> fail x

  [<Test>]
  let ``parse Single`` () =
    match Expr.parse "1.0f" with
    | Factor(Single 1.0f) -> ()
    | x -> fail x

  [<Test>]
  let ``parse Double`` () =
    match Expr.parse "1.0" with
    | Factor(Double 1.0) -> ()
    | x -> fail x

  [<Test>]
  let ``parse Decimal`` () =
    match Expr.parse "1.234M" with
    | Factor(Decimal 1.234M) -> ()
    | x -> fail x

  [<Test>]
  let ``parse String`` () =
    match Expr.parse "'abc'" with
    | Factor(String "abc") -> ()
    | x -> fail x

  [<Test>]
  let ``parse String : single quotation`` () =
    match Expr.parse "'a''bc'" with
    | Factor(String "a'bc") -> ()
    | x -> fail x

  [<Test>]
  let ``parse String : double quotation`` () =
    match Expr.parse "'a\"bc'" with
    | Factor(String "a\"bc") -> ()
    | x -> fail x

  [<Test>]
  let ``parse String : back slash`` () =
    match Expr.parse "'a\bc'" with
    | Factor(String "a\bc") -> ()
    | x -> fail x

  [<Test>]
  let ``parse String : escaped back slash`` () =
    match Expr.parse "'a\\bc'" with
    | Factor(String "a\\bc") -> ()
    | x -> fail x

  [<Test>]
  let ``parse String : new line`` () =
    match Expr.parse "'a\nbc'" with
    | Factor(String "a\nbc") -> ()
    | x -> fail x

  [<Test>]
  let ``parse String : escaped new line`` () =
    match Expr.parse "'a\\nbc'" with
    | Factor(String "a\\nbc") -> ()
    | x -> fail x

  [<Test>]
  let ``parse Var`` () =
    match Expr.parse "abc" with
    | Factor(Var ("abc", _)) -> ()
    | x -> fail x

  [<Test>]
  let ``parse Var : delimited ident`` () =
    match Expr.parse "[あいうえお]" with
    | Factor(Var ("あいうえお", _)) -> ()
    | x -> fail x

  [<Test>]
  let ``parse Add`` () =
    match Expr.parse "1 + 1" with
    | Add(Factor(Int32 1), Factor(Int32 1), _) -> ()
    | x -> fail x

  [<Test>]
  let ``parse Sub`` () =
    match Expr.parse "1 - 1" with
    | Sub(Factor(Int32 1), Factor(Int32 1), _) -> ()
    | x -> fail x

  [<Test>]
  let ``parse Mul`` () =
    match Expr.parse "1 * 1" with
    | Mul(Factor(Int32 1), Factor(Int32 1), _) -> ()
    | x -> fail x

  [<Test>]
  let ``parse Div`` () =
    match Expr.parse "1 / 1" with
    | Div(Factor(Int32 1), Factor(Int32 1), _) -> ()
    | x -> fail x

  [<Test>]
  let ``parse Mod`` () =
    match Expr.parse "1 % 1" with
    | Mod(Factor(Int32 1), Factor(Int32 1), _) -> ()
    | x -> fail x

  [<Test>]
  let ``parse Equal`` () =
    match Expr.parse "1 = 1" with
    | Equal(Factor(Int32 1), Factor(Int32 1), _) -> ()
    | x -> fail x

  [<Test>]
  let ``parse NotEqual`` () =
    match Expr.parse "1 <> 1" with
    | NotEqual(Factor(Int32 1), Factor(Int32 1), _) -> ()
    | x -> fail x

  [<Test>]
  let ``parse LessThan`` () =
    match Expr.parse "1 < 1" with
    | LessThan(Factor(Int32 1), Factor(Int32 1), _) -> ()
    | x -> fail x

  [<Test>]
  let ``parse GreaterThan`` () =
    match Expr.parse "1 > 1" with
    | GreaterThan(Factor(Int32 1), Factor(Int32 1), _) -> ()
    | x -> fail x

  [<Test>]
  let ``parse LessThanOrEqual`` () =
    match Expr.parse "1 <= 1" with
    | LessThanOrEqual(Factor(Int32 1), Factor(Int32 1), _) -> ()
    | x -> fail x

  [<Test>]
  let ``parse GreaterThanOrEqual`` () =
    match Expr.parse "1 >= 1" with
    | GreaterThanOrEqual(Factor(Int32 1), Factor(Int32 1), _) -> ()
    | x -> fail x

  [<Test>]
  let ``parse AndAlso`` () =
    match Expr.parse "true && true" with
    | AndAlso(Factor(Boolean true), Factor(Boolean true), _) -> ()
    | x -> fail x

  [<Test>]
  let ``parse OrElse`` () =
    match Expr.parse "true || true" with
    | OrElse(Factor(Boolean true), Factor(Boolean true), _) -> ()
    | x -> fail x

  [<Test>]
  let ``parse Not`` () =
    match Expr.parse "not true" with
    | Not("not", Factor(Boolean true), _) -> ()
    | x -> fail x

  [<Test>]
  let ``parse Property`` () =
    match Expr.parse "aaa.Length" with
    | Factor(Property(Var ("aaa", _), "Length", _)) -> ()
    | x -> fail x

  [<Test>]
  let ``parse StaticProperty`` () =
    match Expr.parse "$System.DateTime$.Now" with
    | Factor(StaticProperty("System.DateTime", "Now", _)) -> ()
    | x -> fail x

  [<Test>]
  let ``parse Application : 1 argument`` () =
    match Expr.parse "id 1" with
    | Application(Factor(Var ("id", _)), Int32 1, _) -> ()
    | x -> fail x

  [<Test>]
  let ``parse Application : 2 arguments`` () =
    match Expr.parse "add 1 2" with
    | Application(Application(Factor(Var ("add", _)), Int32 1, _), Int32 2, _) -> ()
    | x -> fail x

  [<Test>]
  let ``parse Tuple : 2 elements`` () =
    match Expr.parse "1, 2" with
    | Tuple([Int32(1); Int32(2)], _) -> ()
    | x -> fail x

  [<Test>]
  let ``parse Tuple : 3 elements`` () =
    match Expr.parse "1, 2, 3" with
    | Tuple([Int32(1); Int32(2); Int32(3)], _) -> ()
    | x -> fail x

  [<Test>]
  let ``parse Tuple : empty element`` () =
    try 
      Expr.parse "10, ,1" |> ignore
      fail ()
    with
    | :? ExprException as ex -> 
      printfn "%s" ex.Message
      assert_equal "TRANQ1019" ex.MessageId
    | ex -> 
      fail ex

  [<Test>]
  let ``parse : In`` () =
    match Expr.parse "aaa in bbb" with
    | In(Var("aaa", _), Factor(Var("bbb", _)), _) -> ()
    | x -> fail x

  [<Test>]
  let ``parse Parens`` () =
    match Expr.parse "(1)" with
    | Factor(Parens(Factor(Int32 1))) -> ()
    | x -> fail x

  [<Test>]
  let ``parse Parens : extra a right paren`` () =
    try 
      Expr.parse "(1)) && 2 = 2" |> ignore
      fail ()
    with
    | :? ExprException as ex -> 
      printfn "%s" ex.Message
      assert_equal "TRANQ1019" ex.MessageId
    | ex -> 
      fail ex

  [<Test>]
  let ``parse : precedence for "true && false && not true"`` () =
    match Expr.parse "true && false && not true" with
    | AndAlso(AndAlso(Factor(Boolean true), Factor(Boolean false), _), Not("not", Factor(Boolean true), _), _) -> ()
    | x -> fail x

  [<Test>]
  let ``parse : precedence for "true && false || not true"`` () =
    match Expr.parse "true && false || not true" with
    | OrElse(AndAlso(Factor(Boolean true), Factor(Boolean false), _), Not("not", Factor(Boolean true), _), _) -> ()
    | x -> fail x

  [<Test>]
  let ``parse : precedence for "true || false && not true"`` () =
    match Expr.parse "true || false && not true" with
    | OrElse(Factor(Boolean true), AndAlso(Factor(Boolean false), Not("not", Factor(Boolean true), _), _), _) -> ()
    | x -> fail x

  [<Test>]
  let ``parse : precedence for "true || false || not true"`` () =
    match Expr.parse "true || false || not true" with
    | OrElse(OrElse(Factor(Boolean true), Factor(Boolean false), _), Not("not", Factor(Boolean true), _), _) -> ()
    | x -> fail x

  [<Test>]
  let ``parse : unsupported token`` () =
    try 
      Expr.parse "1 ? 1" |> ignore
      fail ()
    with
    | :? ExprException as ex ->
      printfn "%s" ex.Message
      assert_equal "TRANQ1018" ex.MessageId
    | ex ->
      fail ex

  [<Test>]
  let ``parse : location`` () =
    match Expr.parse "1 = 1" with
    | Equal(Factor(Int32 1), Factor(Int32 1), loc) -> 
      assert_equal { pos_fname = "Expr"; pos_lnum = 1; pos_bol = 0; pos_cnum = 2; } loc
    | x ->
      fail x

  [<Test>]
  let ``evaluate Unit`` () =
    let result = Expr.evaluate "()" Map.empty
    assert_equal () (fst result)
    assert_equal typeof<obj> (snd result) 

  [<Test>]
  let ``evaluate Null`` () =
    let result = Expr.evaluate "null" Map.empty
    assert_equal null (fst result)
    assert_equal typeof<obj> (snd result) 

  [<Test>]
  let ``evaluate Int32`` () =
    let result = Expr.evaluate "1" Map.empty
    assert_equal 1 (fst result)
    assert_equal typeof<Int32> (snd result) 

  [<Test>]
  let ``evaluate String`` () =
    let result = Expr.evaluate "'1'" Map.empty
    assert_equal "1" (fst result)
    assert_equal typeof<String> (snd result) 
        
  [<Test>]
  let ``evaluate Boolean`` () =
    let result = Expr.evaluate "true" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<Boolean> (snd result) 

  [<Test>]
  let ``evaluate Int32 Add Int32`` () =
    let result = Expr.evaluate "1 + 1" Map.empty
    assert_equal 2 (fst result)
    assert_equal typeof<Int32> (snd result) 

  [<Test>]
  let ``evaluate String Add String`` () =
    let result = Expr.evaluate "'abc' + 'd'" Map.empty
    assert_equal "abcd" (fst result)
    assert_equal typeof<String> (snd result) 

  [<Test>]
  let ``evaluate Float Add Float`` () =
    let result = Expr.evaluate "1.0 + 1.0" Map.empty
    assert_equal 2.0 (fst result)
    assert_equal typeof<float> (snd result) 

  [<Test>]
  let ``evaluate Int64 Add Int64`` () =
    let result = Expr.evaluate "1L + 1L" Map.empty
    assert_equal 2L (fst result)
    assert_equal typeof<int64> (snd result) 

  [<Test>]
  let ``evaluate UInt64 Add UInt64`` () =
    let result = Expr.evaluate "1UL + 1UL" Map.empty
    assert_equal 2UL (fst result)
    assert_equal typeof<uint64> (snd result) 

  [<Test>]
  let ``evaluate UInt32 Add UInt32`` () =
    let result = Expr.evaluate "1u + 1u" Map.empty
    assert_equal 2u (fst result)
    assert_equal typeof<uint32> (snd result) 

  [<Test>]
  let ``evaluate Int16 Add Int16`` () =
    let result = Expr.evaluate "1s + 1s" Map.empty
    assert_equal 2s (fst result)
    assert_equal typeof<int16> (snd result) 

  [<Test>]
  let ``evaluate UInt16 Add UInt16`` () =
    let result = Expr.evaluate "1us + 1us" Map.empty
    assert_equal 2us (fst result)
    assert_equal typeof<uint16> (snd result) 

  [<Test>]
  let ``evaluate SByte Add SByte`` () =
    let result = Expr.evaluate "1y + 1y" Map.empty
    assert_equal 2y (fst result)
    assert_equal typeof<sbyte> (snd result) 

  [<Test>]
  let ``evaluate Byte Add Byte`` () =
    let result = Expr.evaluate "1uy + 1uy" Map.empty
    assert_equal 2uy (fst result)
    assert_equal typeof<byte> (snd result) 

  [<Test>]
  let ``evaluate Decimal Add Decimal`` () =
    let result = Expr.evaluate "1.1M + 1.1M" Map.empty
    assert_equal 2.2M (fst result)
    assert_equal typeof<decimal> (snd result) 

  [<Test>]
  let ``evaluate Int32 Equal Int32`` () =
    let result = Expr.evaluate "1 = 1" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "1 = 0" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 

  [<Test>]
  let ``evaluate String Equal String`` () =
    let result = Expr.evaluate "'aaa' = 'aaa'" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "'aaa' = 'bbb'" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 

  [<Test>]
  let ``evaluate Boolean Equal Boolean`` () =
    let result = Expr.evaluate "true = true" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "false = false" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "true = false" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "false = true" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 

  [<Test>]
  let ``evaluate Int32 NotEqual Int32`` () =
    let result = Expr.evaluate "1 <> 1" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "1 <> 0" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 

  [<Test>]
  let ``evaluate String NotEqual String`` () =
    let result = Expr.evaluate "'aaa' <> 'aaa'" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "'aaa' <> 'bbb'" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 

  [<Test>]
  let ``evaluate Boolean NotEqual Boolean`` () =
    let result = Expr.evaluate "true <> true" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "false <> false" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "true <> false" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "false <> true" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 

  [<Test>]
  let ``evaluate Int32 LessThan Int32`` () =
    let result = Expr.evaluate "1 < 1" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "1 < 0" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "0 < 1" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 

  [<Test>]
  let ``evaluate String LessThan String`` () =
    let result = Expr.evaluate "'aaa' < 'aaa'" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "'aaa' < 'bbb'" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "'bbb' < 'aaa'" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 

  [<Test>]
  let ``evaluate Boolean LessThan Boolean`` () =
    let result = Expr.evaluate "true < true" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "true < false" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "false < true" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 


  [<Test>]
  let ``evaluate LessThan : different types`` () =
    try 
      Expr.evaluate "10 < true" Map.empty |> ignore
      fail ()
    with
    | :? ExprException as ex -> 
      printfn "%s" ex.Message
      assert_equal "TRANQ1001" ex.MessageId
    | ex -> 
      fail ex 

  [<Test>]
  let ``evaluate LessThan : not IComparable`` () =
    try 
      Expr.evaluate "a < a" (dict ["a", (obj(), typeof<obj>)] ) |> ignore
      fail ()
    with
    | :? ExprException as ex ->
      printfn "%s" ex.Message
      assert_equal "TRANQ1007" ex.MessageId
    | ex -> fail ex
  
  [<Test>]
  let ``evaluate Int32 GreaterThan Int32`` () =
    let result = Expr.evaluate "1 > 1" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "1 > 0" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "0 > 1" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 

  [<Test>]
  let ``evaluate String GreaterThan String`` () =
    let result = Expr.evaluate "'aaa' > 'aaa'" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "'aaa' > 'bbb'" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "'bbb' > 'aaa'" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 

  [<Test>]
  let ``evaluate Boolean GreaterThan Boolean`` () =
    let result = Expr.evaluate "true > true" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "true > false" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "false > true" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 

  [<Test>]
  let ``evaluate Int32 LessThanOrEqual Int32`` () =
    let result = Expr.evaluate "1 <= 1" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "1 <= 0" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "0 <= 1" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 

  [<Test>]
  let ``evaluate String LessThanOrEqual String`` () =
    let result = Expr.evaluate "'aaa' <= 'aaa'" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "'aaa' <= 'bbb'" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "'bbb' <= 'aaa'" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 

  [<Test>]
  let ``evaluate Boolean LessThanOrEqual Boolean`` () =
    let result = Expr.evaluate "true <= true" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "true <= false" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "false <= true" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 

  [<Test>]
  let ``evaluate Int32 GreaterThanOrEqual Int32`` () =
    let result = Expr.evaluate "1 >= 1" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "1 >= 0" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "0 >= 1" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 

  [<Test>]
  let ``evaluate String GreaterThanOrEqual String`` () =
    let result = Expr.evaluate "'aaa' >= 'aaa'" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "'aaa' >= 'bbb'" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "'bbb' >= 'aaa'" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 

  [<Test>]
  let ``evaluate Boolean GreaterThanOrEqual Boolean`` () =
    let result = Expr.evaluate "true >= true" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "true >= false" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "false >= true" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 

  [<Test>]
  let ``evaluate AndAlso`` () =
    let result = Expr.evaluate "true && true" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "true && false" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "false && true" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "false && false" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 

  [<Test>]
  let ``evaluate AndAlso : different type`` () =
    try
      Expr.evaluate "true && 1" Map.empty |> ignore
      fail ()
    with
    | :? ExprException as ex -> 
      printfn "%s" ex.Message
      assert_equal "TRANQ1009" ex.MessageId 
    | ex -> 
      fail ex

  [<Test>]
  let ``evaluate AndAlso : not boolean`` () =
    try
      Expr.evaluate "1 && 1" Map.empty |> ignore
      fail ()
    with
    | :? ExprException as ex -> 
      printfn "%s" ex.Message
      assert_equal "TRANQ1009" ex.MessageId 
    | ex -> 
      fail ex

  [<Test>]
  let ``evaluate AndAlso : lazy evaluation : lhs not evaluated`` () =
    let result = Expr.evaluate "value <> null && value.Length > 0" (dict ["value", (null, typeof<obj>)]) 
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 

  [<Test>]
  let ``evaluate AndAlso : lazy evaluation : lhs evaluated`` () =
    let result = Expr.evaluate "value <> null && value.Length > 0" (dict ["value", (box "abc", typeof<string>)]) 
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 

  [<Test>]
  let ``evaluate OrElse`` () =
    let result = Expr.evaluate "true || true" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "true || false" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "false || true" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "false || false" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 

  [<Test>]
  let ``evaluate OrElse : lazy evaluation : lhs not evaluated`` () =
    let result = Expr.evaluate "value = null || value.Length = 0" (dict ["value", (null, typeof<obj>)]) 
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 

  [<Test>]
  let ``evaluate OrElse : lazy evaluation : lhs evaluated`` () =
    let result = Expr.evaluate "value = null || value.Length = 0" (dict ["value", (box "abc", typeof<string>)]) 
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 

  [<Test>]
  let ``evaluate Not`` () =
    let result = Expr.evaluate "not true" Map.empty
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "not false" Map.empty
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 

  [<Test>]
  let ``evaluate Not : not boolean 1`` () =
    try
      Expr.evaluate "not 1" Map.empty |> ignore
      fail ()
    with
    | :? ExprException as ex -> 
      printfn "%s" ex.Message
      assert_equal "TRANQ1011" ex.MessageId 
    | ex -> 
      fail ex

  [<Test>]
  let ``evaluate StaticProperty`` () =
    let result = Expr.evaluate "$System.Type$.Delimiter" Map.empty
    assert_equal Type.Delimiter (fst result)
    assert_equal typeof<char> (snd result) 

  [<Test>]
  let ``evaluate StaticProperty : field`` () =
    let result = Expr.evaluate "$System.Int32$.MaxValue" Map.empty
    assert_equal Int32.MaxValue (fst result)
    assert_equal typeof<int> (snd result) 
    let result = Expr.evaluate "$int$.MaxValue" Map.empty
    assert_equal Int32.MaxValue (fst result)
    assert_equal typeof<int> (snd result) 
    let result = Expr.evaluate "$System.DayOfWeek$.Friday" Map.empty
    assert_equal DayOfWeek.Friday (fst result)
    assert_equal typeof<DayOfWeek> (snd result) 

  [<Test>]
  let ``evaluate StaticProperty : not either static property or static field`` () =
    try 
      Expr.evaluate "$System.String$.xxx" Map.empty |> ignore
      fail ()
    with 
    | :? ExprException as ex -> 
      printfn "%s" ex.Message
      assert_equal "TRANQ1004" ex.MessageId
    | ex -> 
      fail ex

  [<Test>]
  let ``evaluate Property`` () =
    let result = Expr.evaluate "'abc'.Length" Map.empty
    assert_equal 3 (fst result)
    assert_equal typeof<int> (snd result) 

  [<Test>]
  let ``evaluate Property : field`` () =
    let result = Expr.evaluate "a.Name" (Map.ofList ["a", (box { Hoge.Name = "aaa" }, typeof<Hoge>);] )
    assert_equal "aaa" (fst result)
    assert_equal typeof<string> (snd result) 

  [<Test>]
  let ``evaluate Property : not either property or field`` () =
    try 
      Expr.evaluate "'abc'.xxx" Map.empty |> ignore
      fail ()
    with 
    | :? ExprException as ex -> 
      printfn "%s" ex.Message
      assert_equal "TRANQ1003" ex.MessageId
    | ex -> 
      fail ex

  [<Test>]
  let ``evaluate Property : GenericDictionary `` () =
    let dict = Dictionary<string, obj>()
    dict.["Aaa"] <- "Hoge"
    let result = Expr.evaluate "a.Aaa" (Map.ofList ["a", (box dict, dict.GetType());] )
    assert_equal "Hoge" (fst result)
    assert_equal typeof<obj> (snd result) 

  type Hoge2 = { dict : IDictionary<string, string> }

  [<Test>]
  let ``evaluate Property : GenericDictionary : nested `` () =
    let hoge = { dict = Dictionary<string, string>() }
    hoge.dict.["Aaa"] <- "Hoge"
    let result = Expr.evaluate "a.dict.Aaa" (Map.ofList ["a", (box hoge, hoge.GetType());] )
    assert_equal "Hoge" (fst result)
    assert_equal typeof<obj> (snd result) 

  [<Test>]
  let ``evaluate Property : Dictionary `` () =
    let dict = Hashtable()
    dict.["Aaa"] <- "Hoge"
    let result = Expr.evaluate "a.Aaa" (Map.ofList ["a", (box dict, dict.GetType());] )
    assert_equal "Hoge" (fst result)
    assert_equal typeof<obj> (snd result) 

  [<Test>]
  let ``evaluate Property : GenericDictionary : null`` () =
    try
      Expr.evaluate "a.Aaa" (Map.ofList ["a", (null, typeof<Hashtable>);] ) |> ignore 
      fail ()
    with
    | :? ExprException as ex -> 
      printfn "%s" ex.Message
      assert_equal "TRANQ1026" ex.MessageId
    | ex -> 
      fail ex

  [<Test>]
  let ``evaluate Property : GenericDictionary : key not found`` () =
    let dict = Dictionary<string, obj>()
    try
      Expr.evaluate "a.Aaa" (Map.ofList ["a", (box dict, dict.GetType());] ) |> ignore
      fail ()
    with
    | :? ExprException as ex -> 
      printfn "%s" ex.Message
      assert_equal "TRANQ1003" ex.MessageId
    | ex -> 
      fail ex

  [<Test>]
  let ``evaluate Property : Dictionary : key not found`` () =
    let dict = Hashtable()
    let result = Expr.evaluate "a.Aaa" (Map.ofList ["a", (box dict, dict.GetType());] )
    assert_equal null (fst result)
    assert_equal typeof<obj> (snd result) 

  [<Test>]
  let ``evaluate Property : Map `` () =
    let m = Map.ofList [("Aaa", "Hoge")]
    let result = Expr.evaluate "a.Aaa" (Map.ofList ["a", (box m, m.GetType());] )
    assert_equal "Hoge" (fst result)
    assert_equal typeof<obj> (snd result) 

  [<Test>]
  let ``evaluate Property : Map : key not found `` () =
    let m = Map.empty
    try
      Expr.evaluate "a.Aaa" (Map.ofList ["a", (box m, m.GetType());] ) |> ignore
      fail ()
    with
    | :? ExprException as ex -> 
      printfn "%s" ex.Message
      assert_equal "TRANQ1003" ex.MessageId
    | ex -> 
      fail ex

  [<Test>]
  let ``evaluate Var`` () =
    let result = Expr.evaluate "a" (dict ["a", (box "abc", typeof<string>)])
    assert_equal "abc" (fst result)
    assert_equal typeof<string> (snd result) 

  [<Test>]
  let ``evaluate Var : can't resolved`` () =
    try 
      Expr.evaluate "a" Map.empty |> ignore
      fail ()
    with
    | :? ExprException as ex -> 
      printfn "%s" ex.Message
      assert_equal "TRANQ1006" ex.MessageId
    | ex -> 
      fail ex

  [<Test>]
  let ``evaluate Ident.Property : string`` () =
    let result = Expr.evaluate "a.Length" (dict ["a", (box "abc", typeof<string>)])
    assert_equal 3 (fst result)
    assert_equal typeof<int> (snd result) 

  [<Test>]
  let ``evaluate Ident.Property : string : null`` () =
    try
      Expr.evaluate "a.Length" (dict ["a", (null, typeof<string>)]) |> ignore
      fail ()
    with
    | :? ExprException as ex -> 
      printfn "%s" ex.Message
      assert_equal "TRANQ1026" ex.MessageId
    | ex -> 
      fail ex

  [<Test>]
  let ``evaluate Ident.Property : record`` () =
    let result = Expr.evaluate "a.Name" (dict ["a", (box { Hoge.Name = "hoge" }, typeof<Hoge>)])
    assert_equal "hoge" (fst result)
    assert_equal typeof<string> (snd result)

  [<Test>]
  let ``evaluate Ident.Property : record : null`` () =
    try
      Expr.evaluate "a.Name" (dict ["a", (null, typeof<Hoge>)]) |> ignore
      fail ()
    with
    | :? ExprException as ex -> 
      printfn "%s" ex.Message
      assert_equal "TRANQ1026" ex.MessageId
    | ex -> 
      fail ex

  [<Test>]
  let ``evaluate Ident.Property : square branket`` () =
    let result = Expr.evaluate "[レコード].[名前]" (dict ["レコード", (box { レコード.名前 = "hoge" }, typeof<レコード>)])
    assert_equal "hoge" (fst result)
    assert_equal typeof<string> (snd result)

  [<Test>]
  let ``evaluate Ident.Property : escaped square branket`` () =
    let result = Expr.evaluate "[レコード2].[名前[]]]" (dict ["レコード2", (box { レコード2.``名前[]`` = "hoge" }, typeof<レコード2>)])
    assert_equal "hoge" (fst result)
    assert_equal typeof<string> (snd result)

  [<Test>]
  let ``evaluate Ident.Property : unclosed square brancket`` () =
    try
      Expr.evaluate "[レコード].[名前" (dict ["レコード", (box { レコード.名前 = "hoge" }, typeof<レコード>)]) |> ignore
      fail ()
    with
    | :? ExprException as ex -> 
      printfn "%s" ex.Message
      assert_equal "TRANQ1027" ex.MessageId
    | ex -> 
      fail ex


  [<Test>]
  let ``evaluate Ident.Property : option`` () =
    let result = Expr.evaluate "a.IsSome" (dict ["a", (box (Some "foo"), typeof<string option>)])
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "a.IsNone" (dict ["a", (box (Some "foo"), typeof<string option>)])
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "a.IsNone" (dict ["a", (box (unbox<string option> None), typeof<string option>)])
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "a.Value" (dict ["a", (box (Some "foo"), typeof<string option>)])
    assert_equal "foo" (fst result)
    assert_equal typeof<string> (snd result) 

  [<Test>]
  let ``evaluate Ident.Property : option : null`` () =
    try
      Expr.evaluate "a.Value" (dict ["a", (box (unbox<string option> None), typeof<string option>)]) |> ignore
      fail ()
    with
    | :? ExprException as ex -> 
      printfn "%s" ex.Message
      assert_equal "TRANQ1026" ex.MessageId
    | ex -> 
      fail ex

  [<Test>]
  let ``evaluate Ident.Property : nullable`` () =
    let result = Expr.evaluate "a.HasValue" (dict ["a", (box (Nullable 1), typeof<int Nullable>)])
    assert_equal true (fst result)
    assert_equal typeof<bool> (snd result) 
    let result = Expr.evaluate "a.HasValue" (dict ["a", (box (Nullable ()), typeof<int Nullable>)])
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 

  [<Test>]
  let ``evaluate Ident.Property : nullable : null`` () =
    let result = Expr.evaluate "a.HasValue" (dict ["a", (box (Nullable ()), typeof<int Nullable>)])
    assert_equal false (fst result)
    assert_equal typeof<bool> (snd result) 

  [<Test>]
  let ``evaluate Application : 1 arg`` () =
    let incr x = x + 1
    let context = dict ["incr", (box incr, incr.GetType());]
    let result = Expr.evaluate "incr 1" context
    assert_equal 2 (fst result)
    assert_equal typeof<int> (snd result) 

  [<Test>]
  let ``evaluate Application : 2 args`` () =
    let add x y = x + y
    let context = dict ["add", (box add, add.GetType());]
    let result = Expr.evaluate "add 1 2" context
    assert_equal 3 (fst result)
    assert_equal typeof<int> (snd result) 

  [<Test>]
  let ``evaluate Application : 3 args`` () =
    let add x y z = x + y + z
    let context = dict ["add", (box add, add.GetType());]
    let result = Expr.evaluate "add 1 2 3" context
    assert_equal 6 (fst result)
    assert_equal typeof<int> (snd result) 

  [<Test>]
  let ``evaluate Tuple`` () =
    let tuple = 1, "abc", true
    let context = dict ["tuple", (box tuple, tuple.GetType());]
    let result = Expr.evaluate "tuple" context
    assert_equal (1, "abc", true) (fst result)
    assert_equal typeof<Tuple<int, string, bool>> (snd result)

  [<Test>]
  let ``evaluate Parens`` () =
    let add x y = x + y
    let context = dict ["add", (box add, add.GetType())]
    let result = Expr.evaluate "add (add 1 2) 3" context
    assert_equal 6 (fst result)
    assert_equal typeof<int> (snd result)

  [<Test>]
  let ``evaluate In`` () =
    let list = [1; 2; 3]
    let context = dict ["list", (box list, list.GetType());]
    let result = Expr.evaluate "x in list" context
    match fst result with
    | :? Tuple<string, seq<obj>> as x ->
      assert_equal "x" x.Item1
      assert_equal 0 (Seq.compareWith (compare) (Seq.cast<int> x.Item2) (list))
    | _ -> fail()
    assert_equal typeof<Tuple<string, seq<obj>>> (snd result)

  [<Test>]
  let ``evaluate In : illegal left hand operand`` () =
    let list = [1; 2; 3]
    try 
      Expr.evaluate "'aaa' in bbb" (dict ["bbb", (box list, list.GetType())]) |> ignore
      fail ()
    with
    | :? ExprException as ex -> 
      printfn "%s" ex.Message
      assert_equal "TRANQ1021" ex.MessageId
    | ex -> 
      fail ex

  [<Test>]
  let ``evaluate In : illegal right hand operand`` () =
    try 
      Expr.evaluate "aaa in bbb" (dict ["bbb", (box 100, typeof<int>)]) |> ignore
      fail ()
    with
    | :? ExprException as ex -> 
      printfn "%s" ex.Message
      assert_equal "TRANQ1022" ex.MessageId
    | ex -> 
      fail ex

  [<Test>]
  let ``evaluate Option`` () =
    let opt = Some 1
    let result = Expr.evaluate "opt" (Map.ofList ["opt", (box opt, opt.GetType())])
    assert_equal (Some 1) (fst result)
    assert_equal typeof<int option> (snd result) 

  type Foo =
    | Aaa = 1

  [<Test>]
  let ``evaluate Enum`` () =
    let context = dict ["a", (box Foo.Aaa, typeof<Foo>)]
    let result = Expr.evaluate "a" context
    assert_equal Foo.Aaa (fst result)
    assert_equal typeof<Foo> (snd result)
