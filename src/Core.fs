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

type Tx<'R> = Tx of (TxContext -> TxState -> TxResult<'R> * TxState)
 
and TxAttr = Required | RequiresNew | Supports | NotSupported

and TxIsolationLevel = ReadUncommitted | ReadCommitted | RepeatableRead | Serializable | Snapshot

and Event = 
  | TxBegin of int * TxAttr * TxIsolationLevel
  | TxCommit of int * TxAttr * TxIsolationLevel
  | TxRollback of int * TxAttr * TxIsolationLevel
  | Sql of int option * PreparedStatement

and Config = {
  Dialect: IDialect
  ConnectionProvider: unit -> DbConnection
  Listener: Event -> unit }

and TxContext = { 
  Config: Config
  Connection: DbConnection
  Transaction: DbTransaction option }

[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module internal TxHelper =

  let inline run (Tx block) ctx state = 
    block ctx state

  let inline returnM x = Tx(fun ctx state -> Success x, state)
  
  let inline bindM m f = Tx(fun ctx state -> 
    match run m ctx state with
    | Success out, state -> run (f out) ctx state
    | Failure exn, state -> Failure exn, state)

type TxBuilder(txAttr: TxAttr, txIsolatioinLevel: TxIsolationLevel) =
  let toAdoTx = function
    | ReadUncommitted -> System.Data.IsolationLevel.ReadUncommitted
    | ReadCommitted -> System.Data.IsolationLevel.ReadCommitted
    | RepeatableRead -> System.Data.IsolationLevel.RepeatableRead
    | Serializable -> System.Data.IsolationLevel.Serializable
    | Snapshot -> System.Data.IsolationLevel.Snapshot
  let confirmOpen (con: DbConnection) =
    if con.State <> ConnectionState.Open then
      con.Open()
    con
  member this.Return(x) = TxHelper.returnM x
  member this.ReturnFrom(m) = m
  member this.Bind(m, f) = TxHelper.bindM m f
  member this.Delay(f) = this.Bind(this.Return(), f)
  member this.Zero() = this.Return()
  member this.Combine(r1, r2) = this.Bind(r1, fun () -> r2)
  member this.TryWith(m, h) = Tx(fun ctx state ->
    try TxHelper.run m ctx state
    with e -> TxHelper.run (h e) ctx state)
  member this.TryFinally(m, compensation) = Tx(fun ctx state ->
    try TxHelper.run m ctx state
    finally compensation())
  member this.Using(res:#IDisposable, body) =
    this.TryFinally(body res, (fun () -> 
      match res with 
      | null -> () 
      | disp -> disp.Dispose()))
  member this.While(guard, m) =
    this.Bind(m, (fun () -> this.While(guard, m)))
  member this.For(sequence:seq<_>, body) =
    this.Using(sequence.GetEnumerator(),
      (fun enum -> this.While(enum.MoveNext, this.Delay(fun () -> body enum.Current))))
  member this.Run(f) = Tx(fun ({Config = config; Connection = con; Transaction = tx} as ctx) state ->
    let listener = config.Listener
    let run ctx state = TxHelper.run f ctx state
    let begin_ (tx: DbTransaction) =
      config.Listener (TxBegin (tx.GetHashCode(), txAttr, txIsolatioinLevel))
    let commit (tx: DbTransaction) =
      tx.Commit()
      config.Listener (TxCommit (tx.GetHashCode(), txAttr, txIsolatioinLevel))
    let rollback (tx: DbTransaction) = 
      tx.Rollback()
      config.Listener (TxRollback (tx.GetHashCode(), txAttr, txIsolatioinLevel))
    let completeTx tx result (state: TxState) =
      match result with
      | Success value -> 
        try
          if state.IsRollbackOnly then
            rollback tx
          else
            commit tx
          Success value, state
        with e ->
          Failure e, state
      | Failure exn ->
        try rollback tx with e -> ()
        Failure exn, state 
    match txAttr, tx with
    | Required, Some _ -> 
      run ctx state
    | Required, None
    | RequiresNew, None ->
      let con = confirmOpen con
      use tx = con.BeginTransaction(toAdoTx txIsolatioinLevel)
      begin_ tx
      let result, state = run {ctx with Connection = con; Transaction = Some tx } {IsRollbackOnly = false}
      completeTx tx result state
    | RequiresNew, Some _ ->
      use con = confirmOpen (config.ConnectionProvider())
      use tx = con.BeginTransaction(toAdoTx txIsolatioinLevel)
      begin_ tx
      let result, state = run {ctx with Connection = con; Transaction = Some tx } {IsRollbackOnly = false}
      completeTx tx result state
    | Supports, _ ->
      run ctx state
    | NotSupported, _ -> 
      use con = confirmOpen (config.ConnectionProvider())
      run {ctx with Connection = con; Transaction = None } {IsRollbackOnly = false})

exception Abort of string

[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Tx =

  let abort e = Tx(fun _ state -> Failure e, state)

  let abortwith message = abort <| Abort message

  let rollbackOnly = Tx(fun _ _ -> Success (), { IsRollbackOnly = true })

  let inline returnM m = TxHelper.returnM m

  let inline applyM f m =
    TxHelper.bindM f <| fun f' ->
      TxHelper.bindM m <| fun m' ->
        returnM (f' m') 

  let inline liftM f m =
    let ret x = returnM (f x)
    TxHelper.bindM m ret

  let inline liftM2 f x y = applyM (applyM (returnM f) x) y

  let inline private cons hd tl = hd :: tl
    
  let inline sequence s =
    let inline cons a b = liftM2 (cons) a b
    List.foldBack cons s (returnM [])

  let inline mapM f x = sequence (List.map f x)

  let inline ignore m = liftM ignore m

  let run (config: Config) (Tx(f)) =
    let state = { IsRollbackOnly = false }
    try
      use con = config.ConnectionProvider()
      let ctx = { 
        Config = config
        Connection = con
        Transaction = None }
      f ctx state
    with e -> 
      Failure e, state

  let eval config f = 
    run config f |> fst

  let exec config f = 
    run config f |> snd

[<AutoOpen>]
module Directives =

  let tx(attr, level) = TxBuilder(attr, level)

  let txRequired = TxBuilder(TxAttr.Required, TxIsolationLevel.ReadCommitted)

  let txRequiresNew = TxBuilder(TxAttr.RequiresNew, TxIsolationLevel.ReadCommitted)

  let txSupports = TxBuilder(TxAttr.Supports, TxIsolationLevel.ReadCommitted)

  let txNotSupported = TxBuilder(TxAttr.NotSupported, TxIsolationLevel.ReadCommitted)

[<AutoOpen>]
module Operators =

  let inline (<--) (name:string) (value:'T) = Param(name, box value, typeof<'T>)

  let inline (<*>) f m = Tx.applyM f m

  let inline (<!>) f m = Tx.liftM f m
