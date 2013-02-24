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

type DataConvRepo() =
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
  abstract DataConvRepo: DataConvRepo
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
  abstract CreateParameterName: index:int -> string
  abstract CreateParameterName: baseName:string -> string
  abstract IsUniqueConstraintViolation: exn:exn -> bool
  abstract RewriteForPagination: statement:SqlAst.Statement * sql:string * condition:IDictionary<string, obj * Type> * offset:int64 * limit:int64 -> string * IDictionary<string, obj * Type>
  abstract RewriteForCalcPagination: statement:SqlAst.Statement * sql:string * condition:IDictionary<string, obj * Type> * offset:int64 * limit:int64 -> string * IDictionary<string, obj * Type>
  abstract RewriteForCount: statement:SqlAst.Statement * sql:string * condition:IDictionary<string, obj * Type> -> string * IDictionary<string, obj * Type>
  abstract BuildProcedureCallSql: procedureName:string * parameters:PreparedParam seq -> string
  abstract EncloseIdentifier: identifier:string -> string
  abstract SetupDbParameter: param:PreparedParam * dbParam:DbParameter -> unit
  abstract GetValue: reader:DbDataReader * index:int * destProp:PropertyInfo -> obj
  abstract MakeParametersDisposer: command:DbCommand -> IDisposable
  abstract ParseSql: text:string -> SqlAst.Statement

type InsertOpt() =
  member val Exclude: string seq = Seq.empty with get, set
  member val Include: string seq = Seq.empty with get,set
  member val ExcludeNull = false with get, set

type UpdateOpt() =
  member val Exclude: string seq = Seq.empty with get, set
  member val Include: string seq = Seq.empty with get,set
  member val ExcludeNull = false with get, set
  member val IgnoreVersion = false with get, set

type DeleteOpt() =
  member val IgnoreVersion = false with get, set

type Param = Param of string * obj * Type

type SqlBuilder(dialect:IDialect, ?capacity, ?parameterNameSuffix) =
  let sql = StringBuilder(defaultArg capacity 200)
  let formattedSql = StringBuilder(defaultArg capacity 200)
  let parameters = ResizeArray<PreparedParam>()
  let parameterNameSuffix = defaultArg parameterNameSuffix String.Empty
  let mutable parameterIndex = 0
  member this.Sql = sql
  member this.FormattedSql = formattedSql
  member this.Parameters = parameters
  member this.ParameterIndex
    with get () = parameterIndex
    and  set (v) = parameterIndex <- v
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
    let parameterName = dialect.CreateParameterName(parameterIndex) + parameterNameSuffix
    parameterIndex <- parameterIndex + 1
    sql.Append(parameterName) |> ignore
    formattedSql.Append (dialect.FormatAsSqlLiteral(value, typ, dbType)) |> ignore
    parameters.Add(
      { Name = parameterName
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

type Config = {
  Dialect: IDialect
  ConnectionProvider: unit -> DbConnection
  Logger: PreparedStatement -> unit }

type TxContext = { 
  Config: Config
  Connection: DbConnection
  Transaction: DbTransaction option }

type TxResult<'R> = Success of 'R | Failure of exn

type TxBlock<'R> = TxBlock of (TxContext -> TxResult<'R>)
 
type TxAttr = Required | RequiresNew | Supports | NotSupported

type TxIsolationLevel = ReadUncommitted | ReadCommitted | RepeatableRead | Serializable | Snapshot

type TxBlockBuilder(txAttr: TxAttr, level: TxIsolationLevel) =
  let toAdoTx = function
    | ReadUncommitted -> System.Data.IsolationLevel.ReadUncommitted
    | ReadCommitted -> System.Data.IsolationLevel.ReadCommitted
    | RepeatableRead -> System.Data.IsolationLevel.RepeatableRead
    | Serializable -> System.Data.IsolationLevel.Serializable
    | Snapshot -> System.Data.IsolationLevel.Snapshot
  let runTxBlock (TxBlock block) ctx = 
    block ctx
  let confirmOpen (con: DbConnection) =
    if con.State <> ConnectionState.Open then
      con.Open()
    con
  member this.Return(result) = TxBlock(fun _ -> Success result)
  member this.ReturnFrom(m) = m
  member this.Bind(m, f) = TxBlock(fun ctx -> 
    match runTxBlock m ctx with
    | Success out -> runTxBlock (f out) ctx
    | Failure exn -> Failure exn)
  member this.Delay(f) = this.Bind(this.Return(), f)
  member this.Zero() = this.Return()
  member this.Combine(r1, r2) = this.Bind(r1, fun () -> r2)
  member this.TryWith(m, h) = TxBlock(fun ctx ->
    try runTxBlock m ctx
    with e -> runTxBlock (h e) ctx)
  member this.TryFinally(m, compensation) = TxBlock(fun ctx ->
    try runTxBlock m ctx
    with e -> compensation(); Success())
  member this.Using(res:#IDisposable, body) =
    this.TryFinally(body res, (fun () -> 
      match res with 
      | null -> () 
      | disp -> disp.Dispose()))
  member this.While(guard, m) =
    if not(guard()) then this.Zero() 
    else this.Bind(m, (fun () -> this.While(guard, m)))
  member this.For(sequence:seq<_>, body) =
    this.Using(sequence.GetEnumerator(),
      (fun enum -> this.While(enum.MoveNext, this.Delay(fun () ->body enum.Current))))
  member this.Run(f) = TxBlock(fun ({Config = {ConnectionProvider = provider}; Connection = con; Transaction = tx} as ctx) ->
    let run ctx = runTxBlock f ctx
    let completeTx result (tx: DbTransaction) =
      match result with
      | Success _ -> tx.Commit()
      | Failure _ -> tx.Rollback()
      result
    match txAttr, tx with
    | Required, Some _ -> 
      run ctx
    | Required, None
    | RequiresNew, None ->
      let con = confirmOpen con
      use tx = con.BeginTransaction(toAdoTx level)
      let result = run {ctx with Connection = con; Transaction = Some tx }
      completeTx result tx
    | RequiresNew, Some _ ->
      use con = confirmOpen (provider())
      use tx = con.BeginTransaction(toAdoTx level)
      let result = run {ctx with Connection = con; Transaction = Some tx }
      completeTx result tx
    | Supports, _ ->
      run ctx
    | NotSupported, _ -> 
      use con = confirmOpen (provider())
      run {ctx with Connection = con; Transaction = None })

exception Abort of string

[<AutoOpen>]
module Operations =

  let inline (<--) (name:string) (value:'T) = Param(name, box value, typeof<'T>)

  let txBlock(attr, level) = TxBlockBuilder(attr, level)

  let txRequired = TxBlockBuilder(TxAttr.Required, TxIsolationLevel.ReadCommitted)

  let txRequiresNew = TxBlockBuilder(TxAttr.RequiresNew, TxIsolationLevel.ReadCommitted)

  let txSupports = TxBlockBuilder(TxAttr.Supports, TxIsolationLevel.ReadCommitted)

  let txNotSupported = TxBlockBuilder(TxAttr.NotSupported, TxIsolationLevel.ReadCommitted)

  let abort e = TxBlock(fun _ -> Failure(e))

  let abortwith message = abort <| Abort message

  let runTxBlock (config: Config) (TxBlock(txBlock)) = 
    try
      use con = config.ConnectionProvider()
      let ctx = { 
        Config = config
        Connection = con
        Transaction = None }
      txBlock ctx
    with
      | e -> Failure e
