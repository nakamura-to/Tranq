﻿//----------------------------------------------------------------------------
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
open System.Collections.Generic
open System.Collections.Concurrent
open System.Data
open System.Data.Common
open System.Reflection
open System.Text

[<AttributeUsage(AttributeTargets.Class)>]
type TableAttribute() = 
  inherit Attribute()
  let mutable isEnclosed:bool = false
  member val Catalog: string = null with get, set
  member val Schema: string = null with get, set
  member val Name: string = null with get, set
  member val IsEnclosed = false with get, set

[<AttributeUsage(AttributeTargets.Property)>]
type ColumnAttribute() = 
  inherit Attribute()
  member val Name: string = null with get, set
  member val Insertable = true with get, set
  member val Updatable = true with get, set
  member val IsEnclosed = false with get, set

[<AttributeUsage(AttributeTargets.Property)>]
type SequenceAttribute() = 
  inherit Attribute()
  member val Catalog: string = null with get, set
  member val Schema: string = null with get, set
  member val Name: string = null with get, set
  member val IsEnclosed = false with get, set
  member val IncrementBy = 1 with get, set

type IdKind =
  | Assigned = 0
  | Identity = 1
  | Sequence = 2

[<AttributeUsage(AttributeTargets.Property)>]
type IdAttribute(kind:IdKind) = 
  inherit Attribute()
  member val Kind = kind with get, set
  new () = new IdAttribute(IdKind.Assigned)

type VersionKind =
  | Incremented = 0
  | Computed = 1

[<AttributeUsage(AttributeTargets.Property)>]
type VersionAttribute(kind:VersionKind) = 
  inherit Attribute()
  member val Kind = kind with get, set
  new () = new VersionAttribute(VersionKind.Incremented)

[<AttributeUsage(AttributeTargets.Class)>]
type ProcedureAttribute() = 
  inherit Attribute()
  member val Catalog:string = null with get, set
  member val Schema:string = null with get, set
  member val Name:string = null with get, set
  member val IsEnclosed = false with get, set

type Direction =
  | Input = 0
  | InputOutput = 1
  | Output = 2
  | ReturnValue = 3
  | Result = 4

[<AttributeUsage(AttributeTargets.Property)>]
type ProcedureParamAttribute() = 
  inherit Attribute()
  let mutable sizeOpt:int option = None
  let mutable precisionOpt:byte option = None
  let mutable scaleOpt:byte option = None
  member val Name:string = null with get, set
  member val Direction = Direction.Input with get, set
  member val UdtTypeName:string = null with get, set
  member this.Size
    with get () = match sizeOpt with Some v -> v | _ -> 0
    and  set (v) = sizeOpt <- Some v
  member this.Precision
    with get () = match precisionOpt with Some v -> v | _ -> 0uy
    and  set (v) = precisionOpt <- Some v
  member this.Scale
    with get () = match scaleOpt with Some v -> v | _ -> 0uy
    and  set (v) = scaleOpt <- Some v
  member this.SizeOpt = sizeOpt
  member this.PrecisionOpt = precisionOpt
  member this.ScaleOpt = scaleOpt

type PreparedParam =
  { Name : string
    Value : obj
    Type : Type
    DbType: DbType
    Direction : Direction
    Size : int option
    Precision : byte option
    Scale : byte option
    UdtTypeName : string }
  override this.ToString() =
    string this.Value

type PreparedStatement =
  { Text : string
    FormattedText : string
    Params : PreparedParam list }

type IDataConv<'TRich, 'TBasic> = 
  abstract Compose : 'TBasic -> 'TRich
  abstract Decompose : 'TRich -> 'TBasic 

type DataConvRegistry() =
  let dict = new ConcurrentDictionary<string, Type * (obj -> obj) * (obj -> obj)>()
  let key (typ: Type) = typ.FullName
  member this.Add<'TRich, 'TBasic>(conv: IDataConv<'TRich, 'TBasic>) =
    let richType = typeof<'TRich>
    if Type.isBasic richType then
      raise <| invalidOp (Message.format (SR.TRANQ5002()))
    let basicType = typeof<'TBasic>
    if not <| Type.isBasic basicType then
      raise <| invalidOp (Message.format (SR.TRANQ5001()))
    let convType = conv.GetType().GetInterface("Tranq.IDataConv`2")
    let composeMethod = convType.GetMethod("Compose")
    let compose basicValue = composeMethod.Invoke(conv, [|basicValue|])
    let decomposeMethod = convType.GetMethod("Decompose")
    let decompose richValue = decomposeMethod.Invoke(conv, [|richValue|])
    let value = basicType, compose, decompose
    dict.AddOrUpdate(key richType, value, fun _ _ -> value) |> ignore
  member this.Remove(richType: Type) =
    dict.TryRemove(key richType) |> ignore
  member this.Remove<'TRich>() =
    this.Remove(typeof<'TRich>)
  member this.TryGet(richType: Type) =
    match dict.TryGetValue(key richType) with
    | true, value -> Some value
    | _ -> None
  member this.TryGet<'TRich>() =
    this.TryGet(typeof<'TRich>)

type IDialect =
  abstract Name: string
  abstract DataConvRegistry: DataConvRegistry
  abstract CanGetIdentityAtOnce: bool
  abstract CanGetIdentityAndVersionAtOnce: bool
  abstract CanGetVersionAtOnce: bool
  abstract IsResultParamRecognizedAsOutputParam: bool
  abstract IsHasRowsPropertySupported: bool
  abstract Env: IDictionary<string, obj * Type>
  abstract EscapeMetaChars: text:string -> string
  abstract PrepareIdentitySelect: tableName:string * idColumnName:string -> PreparedStatement
  abstract PrepareIdentityAndVersionSelect: tableName:string * idColumnName:string * versionColumnName:string -> PreparedStatement
  abstract PrepareVersionSelect: tableName:string * versionColumnName:string * idMetaList:list<string * obj * Type> -> PreparedStatement
  abstract PrepareSequenceSelect: sequenceName:string  -> PreparedStatement
  abstract ConvertFromDbToClr: dbValue:obj * destType:Type * udtTypeName:string * destProp:PropertyInfo-> obj
  abstract ConvertFromClrToDb: clrValue:obj * srcType:Type * udtTypeName:string -> obj * Type * DbType
  abstract FormatAsSqlLiteral: dbValue:obj * clrType:Type * dbType:DbType -> string
  abstract CreateParamName: index:int -> string
  abstract CreateParamName: baseName:string -> string
  abstract IsUniqueConstraintViolation: exn:exn -> bool
  abstract RewriteForPagination: statement:SqlAst.Statement * sql:string * condition:IDictionary<string, obj * Type> * offset:int64 * limit:int64 -> string * IDictionary<string, obj * Type>
  abstract RewriteForCalcPagination: statement:SqlAst.Statement * sql:string * condition:IDictionary<string, obj * Type> * offset:int64 * limit:int64 -> string * IDictionary<string, obj * Type>
  abstract RewriteForCount: statement:SqlAst.Statement * sql:string * condition:IDictionary<string, obj * Type> -> string * IDictionary<string, obj * Type>
  abstract BuildProcedureCallSql: procedureName:string * parameters:PreparedParam seq -> string
  abstract EncloseIdentifier: identifier:string -> string
  abstract SetupDbParam: param:PreparedParam * dbParam:DbParameter -> unit
  abstract GetValue: reader:DbDataReader * index:int * destProp:PropertyInfo -> obj
  abstract MakeParamDisposer: command:DbCommand -> IDisposable
  abstract ParseSql: text:string -> SqlAst.Statement

type Range(?Offset: int64, ?Limit: int64) =
  member this.Offset = defaultArg Offset 0L
  member this.Limit = defaultArg Limit -1L

type InsertOpt(?Exclude: string seq, ?Include: string seq, ?ExcludeNone: bool) =
  member this.Exclude = defaultArg Exclude Seq.empty
  member this.Include = defaultArg Include Seq.empty
  member this.ExcludeNone = defaultArg ExcludeNone false

type UpdateOpt(?Exclude: string seq, ?Include: string seq, ?ExcludeNone: bool, ?IgnoreVersion: bool) =
  member this.Exclude = defaultArg Exclude Seq.empty
  member this.Include = defaultArg Include Seq.empty
  member this.ExcludeNone = defaultArg ExcludeNone false
  member this.IgnoreVersion = defaultArg IgnoreVersion false

type DeleteOpt(?IgnoreVersion: bool) =
  member this.IgnoreVersion = defaultArg IgnoreVersion false

type Param = Param of string * obj * Type

type SqlBuilder(dialect:IDialect, ?capacity, ?paramNameSuffix) =
  let sql = StringBuilder(defaultArg capacity 200)
  let formattedSql = StringBuilder(defaultArg capacity 200)
  let parameters = ResizeArray<PreparedParam>()
  let paramNameSuffix = defaultArg paramNameSuffix String.Empty
  let mutable paramIndex = 0
  member this.Sql = sql
  member this.FormattedSql = formattedSql
  member this.Params = parameters
  member this.ParamIndex
    with get () = paramIndex
    and  set (v) = paramIndex <- v
  member this.Append (fragment : string) =
    sql.Append fragment |> ignore
    formattedSql.Append fragment |> ignore
  member this.Append (fragment : StringBuilder) =
    sql.Append fragment |> ignore
    formattedSql.Append fragment |> ignore
  member this.CutBack (size) =
    sql.Remove(sql.Length - size, size) |> ignore
    formattedSql.Remove(formattedSql.Length - size, size) |> ignore
  member this.Bind (value : obj, typ : Type) =
    let value, typ, dbType = dialect.ConvertFromClrToDb(value, typ, null)
    let paramName = dialect.CreateParamName(paramIndex) + paramNameSuffix
    paramIndex <- paramIndex + 1
    sql.Append(paramName) |> ignore
    formattedSql.Append (dialect.FormatAsSqlLiteral(value, typ, dbType)) |> ignore
    parameters.Add(
      { Name = paramName
        Value = value
        Type = typ
        DbType = dbType
        Direction = Direction.Input
        Size = None
        Precision = None
        Scale = None
        UdtTypeName = null })
  member this.Build () =
    { Text = sql.ToString().Trim()
      FormattedText = formattedSql.ToString().Trim()
      Params = List.ofSeq parameters }

type TxResult<'R> = Success of 'R | Failure of exn

type TxState = { IsRollbackOnly: bool }

type TxAttr = Required | RequiresNew | Supports | NotSupported

type TxIsolationLevel = ReadUncommitted | ReadCommitted | RepeatableRead | Serializable | Snapshot

type TxInfo = { LocalId: string; GlobalId: Guid }

type TxEvent = 
  | TxBegun of TxInfo * TxAttr * TxIsolationLevel
  | TxCommitted of TxInfo * TxAttr * TxIsolationLevel
  | TxRolledback of TxInfo * TxAttr * TxIsolationLevel
  | SqlIssuing of TxInfo option * PreparedStatement

type Tx<'R> = Tx of (TxContext -> TxResult<'R> * TxState)
 
and TxConfig = {
  Dialect: IDialect
  ConnectionProvider: unit -> DbConnection
  Listener: TxEvent -> unit }

and TxContext = { 
  Config: TxConfig
  Connection: DbConnection
  Transaction: DbTransaction option
  TransactionInfo: TxInfo option
  State: TxState }

exception Abort of string

[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Tx =

  /// Marks a transaction as rollback only
  let rollbackOnly = Tx(fun _ -> Success (), { IsRollbackOnly = true })

  /// Gets whether a transaction is marked as rollback only or not
  let isRollbackOnly = Tx(fun {State = state} -> Success state.IsRollbackOnly, state)

  /// Aborts a transaction
  let inline abort e = Tx(fun {State = state} -> Failure e, state)

  /// Aborts a transaction with a message
  let inline abortwith message = abort <| Abort message

  /// Aborts a transaction with a format
  let inline abortwithf fmt  = Printf.ksprintf abortwith fmt

  let runCore (Tx tx) ctx = 
    tx ctx

  let inline returnM m = Tx(fun {State = state} -> Success m, state)

  let inline bindM m f = Tx(fun ctx -> 
    match runCore m ctx with
    | Success out, state -> runCore (f out) {ctx with State = state}
    | Failure exn, state -> Failure exn, state)

  let inline applyM f m =
    bindM f <| fun f' ->
      bindM m <| fun m' ->
        returnM (f' m') 

  let inline liftM f m =
    bindM m <| fun x -> returnM (f x)

  let inline liftM2 f x y = applyM (applyM (returnM f) x) y

  let inline private cons hd tl = hd :: tl
    
  let inline sequence s =
    let inline cons a b = liftM2 (cons) a b
    List.foldBack cons s (returnM [])

  let inline mapM f x = sequence (List.map f x)

  /// Ignores a transactional result
  let inline ignore m = liftM ignore m

  /// Runs a transaction workflow
  let run config workflow =
    let state = { IsRollbackOnly = false }
    try
      use con = config.ConnectionProvider()
      let ctx = { 
        Config = config
        Connection = con
        Transaction = None
        TransactionInfo = None
        State = state }
      runCore workflow ctx
    with e -> 
      Failure e, state

  /// Runs a transaction workflow and gets a result
  let eval config workflow = 
    run config workflow |> fst

  /// Runs a transaction workflow and gets a state
  let exec config workflow = 
    run config workflow |> snd

type TxBuilder(txAttr: TxAttr, txIsolatioinLevel: TxIsolationLevel) =
  let adoIsolationLevel = function
    | ReadUncommitted -> System.Data.IsolationLevel.ReadUncommitted
    | ReadCommitted -> System.Data.IsolationLevel.ReadCommitted
    | RepeatableRead -> System.Data.IsolationLevel.RepeatableRead
    | Serializable -> System.Data.IsolationLevel.Serializable
    | Snapshot -> System.Data.IsolationLevel.Snapshot
  member this.Return(x) = Tx.returnM x
  member this.ReturnFrom(m) = m
  member this.Bind(m, f) = Tx.bindM m f
  member this.Delay(f) = this.Bind(this.Return(), f)
  member this.Zero() = this.Return()
  member this.Combine(r1, r2) = this.Bind(r1, fun () -> r2)
  member this.TryWith(m, h) = Tx(fun ctx ->
    try Tx.runCore m ctx
    with e -> Tx.runCore (h e) ctx)
  member this.TryFinally(m, compensation) = Tx(fun ctx ->
    try Tx.runCore m ctx
    finally compensation())
  member this.Using(res:#IDisposable, body) =
    this.TryFinally(body res, (fun () -> 
      match res with 
      | null -> () 
      | disp -> disp.Dispose()))
  member this.While(guard, m) =
    if not (guard()) then this.Zero() 
    else this.Bind(m, (fun _ -> this.While(guard, m)))
  member this.For(sequence:seq<_>, body) =
    this.Using(sequence.GetEnumerator(),
      (fun enum -> this.While(enum.MoveNext, this.Delay(fun () -> body enum.Current))))
  abstract Run<'T> : Tx<'T> -> Tx<'T>
  default this.Run(f) = Tx(fun ({Config = config; Connection = con; Transaction = tx; State = state} as ctx) ->
    let listener = config.Listener
    let runCore ctx = Tx.runCore f ctx
    let notifyBegin txInfo =
      config.Listener (TxBegun (txInfo, txAttr, txIsolatioinLevel))
    let notifyCommit txInfo =
      config.Listener (TxCommitted (txInfo, txAttr, txIsolatioinLevel))
    let notifyRollback txInfo = 
      config.Listener (TxRolledback (txInfo, txAttr, txIsolatioinLevel))
    let complete (tx: DbTransaction) txInfo (result, state) =
      match result with
      | Success value -> 
        try
          if state.IsRollbackOnly then
            tx.Rollback()
            notifyRollback txInfo
          else
            tx.Commit()
            notifyCommit txInfo
          Success value, state
        with e ->
          Failure e, state
      | Failure exn ->
        try 
          tx.Rollback() 
          notifyRollback txInfo
        with e -> ()
        Failure exn, state 
    match txAttr, tx with
    | Required, Some _
    | Supports, _ ->
      runCore ctx
    | Required, None
    | RequiresNew, None ->
      con.ConfirmOpen()
      use tx = con.BeginTransaction(adoIsolationLevel txIsolatioinLevel)
      let txInfo = { LocalId = string <| tx.GetHashCode(); GlobalId = Guid.Empty }
      notifyBegin txInfo
      runCore {
        ctx with 
          Connection = con
          Transaction = Some tx
          TransactionInfo = Some txInfo
          State = {IsRollbackOnly = false}}
      |> complete tx txInfo
    | RequiresNew, Some _ ->
      use con = config.ConnectionProvider()
      con.ConfirmOpen()
      use tx = con.BeginTransaction(adoIsolationLevel txIsolatioinLevel)
      let txInfo = { LocalId = string <| tx.GetHashCode(); GlobalId = Guid.Empty }
      notifyBegin txInfo
      runCore { 
        ctx with 
          Connection = con
          Transaction = Some tx
          TransactionInfo = Some txInfo
          State = {IsRollbackOnly = false}}
      |> complete  tx txInfo
    | NotSupported, _ -> 
      use con = config.ConnectionProvider()
      con.ConfirmOpen()
      runCore {
        ctx with 
          Connection = con
          Transaction = None
          TransactionInfo = None
          State = {IsRollbackOnly = false}})
  abstract With : TxIsolationLevel -> TxBuilder 
  default this.With(newTxIsolatioinLevel) = TxBuilder(txAttr, newTxIsolatioinLevel)

[<AutoOpen>]
module Directives =

  let internal defaultIsolationLevel = TxIsolationLevel.ReadCommitted

  let txRequired = TxBuilder(TxAttr.Required, defaultIsolationLevel)

  let txRequiresNew = TxBuilder(TxAttr.RequiresNew, defaultIsolationLevel)

  let txSupports = TxBuilder(TxAttr.Supports, defaultIsolationLevel)

  let txNotSupported = TxBuilder(TxAttr.NotSupported, defaultIsolationLevel)

[<AutoOpen>]
module Operators =

  /// Makes a named parameter
  let inline (<--) (name:string) (value:'T) = Param(name, box value, typeof<'T>)

  /// applyM
  let inline (<*>) f m = Tx.applyM f m

  /// liftM
  let inline (<!>) f m = Tx.liftM f m