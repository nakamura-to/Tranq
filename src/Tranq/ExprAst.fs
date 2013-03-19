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

namespace Tranq

open System
open Microsoft.FSharp.Text.Lexing
open Microsoft.FSharp.Text.Parsing
open Tranq.Text

module internal ExprAst =

  type internal Expr =
    | Factor of Factor
    | Add of Expr * Expr * Location
    | Sub of Expr * Expr * Location
    | Mul of Expr * Expr * Location
    | Div of Expr * Expr * Location
    | Mod of Expr * Expr * Location
    | Equal of Expr * Expr * Location
    | NotEqual of Expr * Expr * Location
    | LessThan of Expr * Expr * Location
    | GreaterThan of Expr * Expr * Location
    | LessThanOrEqual of Expr * Expr * Location
    | GreaterThanOrEqual of Expr * Expr * Location
    | AndAlso of Expr * Expr * Location
    | OrElse of Expr * Expr * Location
    | Not of string * Expr * Location
    | Application of Expr * Factor * Location
    | Tuple of Factor list * Location
    | In of Factor * Expr * Location

  and internal Factor =
    | Null
    | Unit
    | Boolean of bool
    | Byte of byte
    | SByte of sbyte 
    | Int16 of int16
    | UInt16 of uint16 
    | Int32 of int32 
    | UInt32 of uint32 
    | Int64 of int64 
    | UInt64 of uint64 
    | Single of single 
    | Double of double 
    | Decimal of decimal 
    | String of string
    | Var of string * Location
    | Parens of Expr
    | Property of Factor * string * Location
    | StaticProperty of string * string * Location

  exception internal ExprParseError of Message * Location

  exception internal ExprParseErrorWithContext of obj

  module internal LexHelper =

    let getLocation (position : Position) =
      { Location.pos_fname = position.pos_fname 
        pos_lnum = position.pos_lnum
        pos_bol = position.pos_bol
        pos_cnum = position.pos_cnum }

    let handleUnclosedSingleQuote position =
      let loc = getLocation position
      raise <| ExprParseError (SR.TRANQ1012 (), loc)

    let handleUnclosedSquareBranket position =
      let loc = getLocation position
      raise <| ExprParseError (SR.TRANQ1027 (), loc)

    let handleUnclosedDollarMark position = 
      let loc = getLocation position
      raise <| ExprParseError (SR.TRANQ1013 (), loc)

    let handleUnsupportedToken position (token:string) =
      let loc = getLocation position
      raise <| ExprParseError (SR.TRANQ1018 token, loc)

  module internal ParseHelper = 

    let getLocation (parseState : IParseState) index =
      let p = parseState.InputStartPosition(index)
      { Location.pos_fname = p.pos_fname 
        pos_lnum = p.pos_lnum
        pos_bol = p.pos_bol
        pos_cnum = p.pos_cnum }

    let newFactor (parseState : IParseState) factor =
      Factor factor

    let newAdd parseState lhs rhs =
      Add (lhs, rhs, getLocation parseState 2)

    let newSub parseState lhs rhs =
      Sub (lhs, rhs, getLocation parseState 2)

    let newMul parseState lhs rhs =
      Mul (lhs, rhs, getLocation parseState 2)

    let newDiv parseState lhs rhs =
      Div (lhs, rhs, getLocation parseState 2)

    let newMod parseState lhs rhs =
      Mod (lhs, rhs, getLocation parseState 2)

    let newEqual parseState lhs rhs =
      Equal (lhs, rhs, getLocation parseState 2)

    let newNotEqual parseState lhs rhs =
      NotEqual (lhs, rhs, getLocation parseState 2)

    let newLessThan parseState lhs rhs =
      LessThan (lhs, rhs, getLocation parseState 2)

    let newGreaterThan parseState lhs rhs =
      GreaterThan (lhs, rhs, getLocation parseState 2)

    let newLessThanOrEqual parseState lhs rhs =
      LessThanOrEqual (lhs, rhs, getLocation parseState 2)

    let newGreaterThanOrEqual parseState lhs rhs =
      GreaterThanOrEqual (lhs, rhs, getLocation parseState 2)

    let newAndAlso parseState lhs rhs =
      AndAlso (lhs, rhs, getLocation parseState 2)

    let newOrElse parseState lhs rhs =
      OrElse (lhs, rhs, getLocation parseState 2)

    let newNot parseState op expression =
      Not (op, expression, getLocation parseState 1)

    let newApplication parseState func arg =
      Application (func, arg, getLocation parseState 1)

    let newTuple parseState factors =
      Tuple (List.rev factors, getLocation parseState 1)

    let newIn parseState factor expression =
      In (factor, expression, getLocation parseState 2)

    let newNull (parseState:IParseState) =
      Null

    let newBoolean (parseState:IParseState) value =
      Boolean value

    let failToParseValue (parseState:IParseState) (value:string) (typ:Type) =
      raise <| ExprParseError (SR.TRANQ1000 (value, typ.Name), (getLocation parseState 1))

    let newByte (parseState:IParseState) (value:string) =
      match Byte.TryParse value with
      | true, x -> Byte (x)
      | _ -> failToParseValue parseState value typeof<Byte>

    let newSByte (parseState:IParseState) (value:string) =
      match SByte.TryParse value with
      | true, x -> SByte (x)
      | _ -> failToParseValue parseState value typeof<SByte>

    let newInt16 (parseState:IParseState) (value:string) =
      match Int16.TryParse value with
      | true, x -> Int16 (x)
      | _ -> failToParseValue parseState value typeof<Int16>

    let newUInt16 (parseState:IParseState) (value:string) =
      match UInt16.TryParse value with
      | true, x -> UInt16 (x)
      | _ -> failToParseValue parseState value typeof<UInt16>

    let newInt32 (parseState:IParseState) (value:string) =
      match Int32.TryParse value with
      | true, x -> Int32 (x)
      | _ -> failToParseValue parseState value typeof<Int32>

    let newUInt32 (parseState:IParseState) (value:string) =
      match UInt32.TryParse value with
      | true, x -> UInt32 (x)
      | _ -> failToParseValue parseState value typeof<UInt32>

    let newInt64 (parseState:IParseState) (value:string) =
      match Int64.TryParse value with
      | true, x -> Int64 (x)
      | _ -> failToParseValue parseState value typeof<Int64>

    let newUInt64 (parseState:IParseState) (value:string) =
      match UInt64.TryParse value with
      | true, x -> UInt64 (x)
      | _ -> failToParseValue parseState value typeof<UInt64>

    let newSingle (parseState:IParseState) (value:string) =
      match Single.TryParse value with
      | true, x -> Single (x)
      | _ -> failToParseValue parseState value typeof<Single>

    let newDouble (parseState:IParseState) (value:string) =
      match Double.TryParse value with
      | true, x -> Double (x)
      | _ -> failToParseValue parseState value typeof<Double>

    let newDecimal (parseState:IParseState) (value:string) =
      match Decimal.TryParse value with
      | true, x -> Decimal (x)
      | _ -> failToParseValue parseState value typeof<Decimal>

    let newString (parseState:IParseState) value =
      String value

    let newVar (parseState:IParseState) value =
      Var (value, getLocation parseState 1)

    let newUnit (parseState:IParseState) =
      Unit

    let newParens (parseState:IParseState) exp =
      Parens exp

    let newProperty parseState instance propName =
      Property (instance, propName, getLocation parseState 3)

    let newStaticProperty parseState typeName propName =
      StaticProperty (typeName, propName, getLocation parseState 3)

    let raiseError (ctxt:ParseErrorContext<_>) =
      raise <| ExprParseErrorWithContext (box ctxt) |> ignore

    let parse_error_rich = Some(raiseError)