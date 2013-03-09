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
open System.Text
open System.Reflection
open System.Runtime.CompilerServices;
open System.Collections
open System.Collections.Generic
open Microsoft.FSharp.Reflection
open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Quotations.Patterns

module internal Guard =

  let inline argNotNull (arg, parameterName) =
    match box arg with
    | null -> invalidArg parameterName ""
    | _ -> ()

module internal Dict =

  let toSeq d = d |> Seq.map (function (KeyValue(pair)) -> pair)
    
  let toArray d = d |> toSeq |> Seq.toArray
    
  let toList d = d |> toSeq |> Seq.toList

  let getDictType (typ:Type) =
    if typ.IsInterface 
       && typ.IsGenericType
       && typ.GetGenericTypeDefinition() = typedefof<IDictionary<_, _>> then
      typ
    else 
      typ.GetInterface "System.Collections.Generic.IDictionary`2"

  let isDict (typ:Type) =
    (getDictType typ) <> null

  let getDictValue (name:string, obj:obj, typ:Type) =
    let typ = getDictType typ
    let m = typ.GetMethod("TryGetValue")
    let args = [| box name; null |]
    match m.Invoke(obj, args) with
    | :? bool as b when b -> Some (args.[1])
    | _ -> None

module internal Map =

  let ofDict d = d |> Seq.map (function (KeyValue(pair)) -> pair) |> Map.ofSeq

module internal Seq =

  let peek (source:seq<'T>) = seq {
    use ie = source.GetEnumerator() 
    if ie.MoveNext() then
      let iref = ref ie.Current
      while ie.MoveNext() do
        let j = ie.Current 
        yield (!iref, true)
        iref := j
      yield (!iref, false) }

  type private ToList =
    static member Invoke<'T>(seq:seq<'T>) =
      use e = seq.GetEnumerator()
      let mutable res = [] 
      while e.MoveNext() do
          res <- e.Current :: res
      List.rev res

  type private ToResizeArray =
    static member Invoke<'T>(seq:seq<'T>) =
      ResizeArray (seq)

  let private rethrow ex =
    let m = typeof<Exception>.GetMethod("PrepForRemoting", BindingFlags.NonPublic ||| BindingFlags.Instance)
    m.Invoke(ex, [||]) |> ignore
    raise ex

  let private changeTo (seq:seq<obj>) (elementTyp:Type) (helperType:Type) =
    let m = typeof<System.Linq.Enumerable>.GetMethod("Cast")
    let m = m.MakeGenericMethod elementTyp
    let seq = m.Invoke(null, [| seq |])
    let m = helperType.GetMethod("Invoke", BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Static)
    let m = m.MakeGenericMethod (elementTyp)
    try
      m.Invoke(null, [| seq |])
    with
      | :? TargetInvocationException as e -> rethrow e.InnerException

  let changeToList (elementTyp:Type) (seq:seq<obj>) =
    changeTo seq elementTyp typeof<ToList>

  let changeToResizeArray (elementTyp:Type) (seq:seq<obj>) =
    changeTo seq elementTyp typeof<ToResizeArray>

module internal Option =

  let asOption<'T when 'T : null and 'T : equality> (value:'T) =
    if value <> null then
      Some value
    else 
      None

  let getOptionInfo optType =
    let cases = FSharpType.GetUnionCases(optType)
    let none = cases.[0]
    let some = cases.[1]
    none, some, some.DeclaringType.GetGenericArguments().[0]

  let isOptionType (typ:Type) = 
    typ.IsGenericType
    && not typ.IsGenericTypeDefinition
    && typ.GetGenericTypeDefinition() = typedefof<option<_>>

  let getElementType (typ:Type) = 
    typ.GetGenericArguments().[0]

  let make optType (elementValue:obj) = 
    let none, some, elementType = getOptionInfo optType
    if elementValue = null then
      FSharpValue.MakeUnion(none, [||])
    else
      let value = Convert.ChangeType(elementValue, elementType)
      FSharpValue.MakeUnion(some, [|value|])

  let getElement (optType:Type) (optValue:obj) =
    let none, some, elementType = getOptionInfo optType
    let _, fields = FSharpValue.GetUnionFields(optValue, optType)
    let elementValue = if fields.Length > 0 then fields.[0] else null
    elementValue, elementType

  type MaybeBuilder() =
    member this.Bind (x, f) =
      match x with 
      | Some y -> f y
      | None -> None
    member this.Return (x) = Some x
    member this.Zero () = None
  
  let maybe = MaybeBuilder()

module internal Number =

  let isNumberType typ =
    typ = typeof<int32> 
    || typ = typeof<int64>
    || typ = typeof<decimal>
    || typ = typeof<uint32>
    || typ = typeof<uint64>
    || typ = typeof<int16>
    || typ = typeof<uint16>
    || typ = typeof<single>
    || typ = typeof<double>
    || typ = typeof<byte>
    || typ = typeof<sbyte>

  let one typ =
    if typ = typeof<int32> then box 1
    elif typ = typeof<int64> then box 1L
    elif typ = typeof<decimal> then box 1M
    elif typ = typeof<uint32> then box 1u
    elif typ = typeof<uint64> then box 1UL
    elif typ = typeof<int16> then box 1s
    elif typ = typeof<uint16> then box 1us
    elif typ = typeof<single> then box 1.f
    elif typ = typeof<double> then box 1.
    elif typ = typeof<byte> then box 1uy
    elif typ = typeof<sbyte> then box 1y
    else invalidArg "typ" ("unsupported " + (string typ))

  let incr (value:obj) =
    match value with
    | :? int32 as v -> box (v + 1)
    | :? int64 as v -> box (v + 1L)
    | :? decimal as v -> box (v + 1M)
    | :? uint32 as v -> box (v + 1u)
    | :? uint64 as v -> box (v + 1UL)
    | :? int16 as v -> box (v + 1s)
    | :? uint16 as v -> box (v + 1us)
    | :? single as v -> box (v + 1.f)
    | :? double as v -> box (v + 1.)
    | :? byte as v -> box (v + 1uy)
    | :? sbyte as v -> box (v + 1y)
    | _ -> invalidArg "obj" (value.ToString())

  let lessThan (lhs:obj, rhs) =
    match lhs with
    | :? int32 as x -> x < (int32 rhs)
    | :? int64 as x -> x < (int64 rhs)
    | :? decimal as x -> x < (decimal rhs)
    | :? uint32 as x -> x < (uint32 rhs)
    | :? uint64 as x -> x < (uint64 rhs)
    | :? int16 as x -> x < (int16 rhs)
    | :? uint16 as x -> x < (uint16 rhs)
    | :? single as x -> x < (single rhs)
    | :? double as x -> x < (double rhs)
    | :? byte as x -> x < (byte rhs)
    | :? sbyte as x -> x < (sbyte rhs)
    | _ -> false


module internal Basic = 

  let private getUnderlyingType typ =
    if Option.isOptionType typ then
      let elementType = Option.getElementType typ
      if elementType.IsEnum then
        Enum.GetUnderlyingType elementType
      else
        elementType
    elif typ.IsEnum then
      Enum.GetUnderlyingType typ
    else
      typ

  let isBasicType typ =
    let t = getUnderlyingType typ
    Number.isNumberType t 
    || t = typeof<string>
    || t = typeof<bool>
    || t = typeof<byte[]>
    || t = typeof<Guid>
    || t = typeof<DateTime>
    || t = typeof<TimeSpan>
    || t = typeof<DateTimeOffset>

module internal Type =

  let isBasic = Basic.isBasicType

  let isRecord = FSharpType.IsRecord

  let isTuple typ = 
    FSharpType.IsTuple typ 
    && FSharpType.GetTupleElements typ 
       |> Array.forall (fun t -> isBasic t || isRecord t)

  let isOption = Option.isOptionType

  let isNumber = Number.isNumberType

[<AutoOpen>]
module internal Extension =

  open System.Data
  open System.Data.Common
  
  type DbConnection with
    member this.ConfirmOpen() =
      if this.State <> ConnectionState.Open then
        this.Open()

[<AutoOpen>]
module internal Util =
  
  let inline (+>) (buf:StringBuilder) (text:string) = buf.Append(text)
  
  let inline (+!) (buf:StringBuilder) (text:string) = buf.Append(text) |> ignore


