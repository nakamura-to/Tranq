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
open System.Reflection
open System.Collections.Concurrent
open System.Collections.Generic
open Microsoft.FSharp.Reflection

type SequenceMeta = 
  { SequenceName : string
    SqlSequenceName : string
    Generate : string -> (unit -> obj) -> int64 }

type IdCase =
  | Assigned
  | Identity
  | Sequence of SequenceMeta

type PropCase =
  | Id of IdCase
  | Version of VersionKind
  | Basic

type PropMeta = 
  { Index : int
    PropName : string
    ColumnName : string
    SqlColumnName : string
    IsInsertable : bool
    IsUpdatable : bool
    PropCase : PropCase
    Type : Type
    Property : PropertyInfo
    GetValue : obj -> obj }

type PreInsertCase =
  | GetSequenceAndInitVersion of PropMeta * SequenceMeta * PropMeta
  | GetSequence of PropMeta * SequenceMeta
  | InitVersion of PropMeta

type InsertCase =
  | Insert_GetIdentityAndVersionAtOnce of PropMeta * PropMeta
  | Insert_GetIentityAtOnce of PropMeta
  | Insert_GetVersionAtOnce of PropMeta
  | Insert_GetIdentityAndVersionLater of PropMeta * PropMeta
  | Insert_GetIdentityLater of PropMeta
  | Insert_GetVersionLater of PropMeta
  | Insert

type UpdateCase =
  | Update_GetVersionAtOnce of PropMeta
  | Update_GetVersionLater of PropMeta
  | Update_IncrementVersion of PropMeta
  | Update

type EntityMeta = 
  { EntityName : string
    TableName : string
    SqlTableName : string
    PropMetaList : PropMeta list
    IdPropMetaList : PropMeta list
    VersionPropMeta : PropMeta option
    Type : Type
    PreInsertCase : PreInsertCase option
    InsertCase : InsertCase
    UpdateCase : UpdateCase
    MakeEntity : obj[] -> obj }

type BasicElementMeta = 
  { Index : int
    Type : Type }

type EntityElementMeta = 
  { Index : int
    EntityMeta : EntityMeta }

type TupleMeta = 
  { BasicElementMetaList : BasicElementMeta list
    EntityElementMetaList : EntityElementMeta list
    Type : Type
    MakeTuple : obj[] -> obj }

type ResultElementCase =
  | EntityType of EntityMeta
  | TupleType of TupleMeta

type ParamMetaCase =
  | Unit
  | Input
  | InputOutput
  | Output
  | ReturnValue
  | Result of ResultElementCase * (seq<obj> -> obj)

type ProcedureParamMeta =
  { Index : int
    ParamName : string 
    Type : Type
    ParamMetaCase : ParamMetaCase
    Size : int option
    Precision : byte option
    Scale : byte option
    UdtTypeName : string
    Property : PropertyInfo
    GetValue : obj -> obj }

type ProcedureMeta =
  { ProcedureName : string
    SqlProcedureName : string
    ProcedureParamMetaList : ProcedureParamMeta list
    MakeProcedure : obj[] -> obj }

type internal MetaException (message, ?innerException:exn) =
  inherit InvalidOperationException (Message.format message, match innerException with Some ex -> ex | _ -> null)
  member this.MessageId = message.Id

[<RequireQualifiedAccess>]
module internal MetaHelper =

  let inline getDbObjectName (attr: ^a) defaultName (dialect:IDialect) =
    let catalog = (^a : (member Catalog: string) attr)
    let schema = (^a : (member Schema: string) attr)
    let name = (^a : (member Name: string) attr)
    let simpleName = if name <> null then name else defaultName
    let nameList =
      [ Option.asOption catalog; Option.asOption schema; Some simpleName ]
      |> List.choose id
    let concat x y = x + "." + y
    let fullName = nameList |> List.reduce concat
    let enclosedFullName = nameList |> List.map dialect.EncloseIdentifier |> List.reduce concat
    fullName, enclosedFullName, simpleName

[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module SequenceMeta =

  let private handleSequenceAttr simpleTableName dialect (prop: PropertyInfo) =
    match Attribute.GetCustomAttribute(prop, typeof<SequenceAttribute>) with 
    | :? SequenceAttribute as attr ->
      let defaultName = simpleTableName + "_SEQ"
      let sequenceName, enclosedSequenceName, _ = MetaHelper.getDbObjectName attr defaultName dialect
      sequenceName, enclosedSequenceName, attr.IsEnclosed, int64 attr.IncrementBy
    | _ -> 
      raise <| MetaException(SR.TRANQ3009 (prop.Name, prop.DeclaringType.FullName))

  let private makeGenerator incrementBy =
    let baseRef = ref 0L
    let stepRef = ref Int64.MaxValue 
    let generateExclusive (lockObj:obj) (executor:unit -> obj) = 
      lock (lockObj) (fun () ->
        let step = !stepRef
        if step < incrementBy then
          stepRef := step + 1L
          !baseRef + step
        else 
          let value = executor ()
          let ``base`` = Convert.ChangeType(value, typeof<int64>) :?> int64
          baseRef := ``base``
          stepRef := 1L
          ``base`` )
    let contextCache = ConcurrentDictionary<string, (unit -> obj) -> int64>()
    fun contextKey (executor:unit -> obj) ->
      let context = contextCache.GetOrAdd(contextKey, generateExclusive (obj()))
      context executor 
    
  let make simpleTableName dialect prop =
    let sequenceName, enclosedSequenceName, isEnclosed, incrementBy = 
      handleSequenceAttr simpleTableName dialect prop
    { SequenceName = sequenceName 
      SqlSequenceName = if isEnclosed then enclosedSequenceName else sequenceName
      Generate = makeGenerator incrementBy }

[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module PropMeta =

  let private handleColumnAttr (prop: PropertyInfo) =
    match Attribute.GetCustomAttribute(prop, typeof<ColumnAttribute>) with 
    | :? ColumnAttribute as attr ->
      let name = if attr.Name <> null then attr.Name else prop.Name
      name, attr.Insertable, attr.Updatable, attr.IsEnclosed
    | _ -> 
      prop.Name, true, true, false

  let private getPropCase simpleTableName dialect (prop: PropertyInfo) =
    match Attribute.GetCustomAttribute(prop, typeof<IdAttribute>) with 
    | :? IdAttribute as attr ->
      match Attribute.GetCustomAttribute(prop, typeof<VersionAttribute>) with 
      | :? VersionAttribute -> 
        raise <| MetaException(SR.TRANQ3005 (prop.Name, prop.DeclaringType.FullName))
      | _ -> 
        match attr.Kind with
        | IdKind.Assigned -> Id Assigned
        | IdKind.Identity -> Id Identity
        | IdKind.Sequence -> Id (Sequence (SequenceMeta.make simpleTableName dialect prop))
        | _ -> failwith "unreachable."
    | _ ->
      match Attribute.GetCustomAttribute(prop, typeof<VersionAttribute>) with 
      | :? VersionAttribute as attr ->
        PropCase.Version attr.Kind
      | _ ->
        PropCase.Basic

  let make simpleTableName dialect index prop =
    let columnName, isInsertable, isUpdatable, isEnclosed = handleColumnAttr prop
    let propCase = getPropCase simpleTableName dialect prop
    { Index = index
      PropName = prop.Name
      ColumnName = columnName
      SqlColumnName = if isEnclosed then dialect.EncloseIdentifier columnName else columnName
      IsInsertable = isInsertable
      IsUpdatable = isUpdatable
      PropCase = propCase
      Type = prop.PropertyType
      Property = prop
      GetValue = FSharpValue.PreComputeRecordFieldReader prop }

[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module EntityMeta =

  let private (|IdentityId|_|) (idPropMetaList:PropMeta list) =
    if idPropMetaList.Length = 1 then
      let idPropMeta = idPropMetaList.Head
      match idPropMeta.PropCase with
      | Id Identity ->
        Some idPropMeta
      | _ ->
        None
    else
      None

  let private (|SequenceId|_|) (idPropMetaList:PropMeta list) =
    if idPropMetaList.Length = 1 then
      let idPropMeta = idPropMetaList.Head
      match idPropMeta.PropCase with
      | Id (Sequence (sequenceMeta)) ->
        Some (idPropMeta, sequenceMeta)
      | _ ->
        None
    else
      None

  let private (|IncremetedVersion|_|) (propMeta:PropMeta option) =
    propMeta
    |> Option.bind (fun propMeta -> 
      match propMeta.PropCase with
      | Version VersionKind.Incremented -> Some propMeta 
      | _ -> None)

  let private (|ComputedVersion|_|) (propMeta:PropMeta option) =
    propMeta
    |> Option.bind (fun propMeta -> 
      match propMeta.PropCase with
      | Version VersionKind.Computed -> Some propMeta 
      | _ -> None)

  let private getPreInsertCase (dialect:IDialect) = function
    | SequenceId (idPropMeta, sequenceMeta), IncremetedVersion versionPropMeta ->
      Some (GetSequenceAndInitVersion (idPropMeta, sequenceMeta, versionPropMeta))
    | SequenceId (idPropMeta, sequenceMeta), _ ->
      Some (GetSequence (idPropMeta, sequenceMeta))
    | _, IncremetedVersion versionPropMeta ->
      Some (InitVersion versionPropMeta)
    | _ ->
      None

  let private getInsertCase (dialect:IDialect) = function
    | IdentityId idPropMeta, ComputedVersion versionPropMeta ->
      if dialect.CanGetIdentityAndVersionAtOnce then
        Insert_GetIdentityAndVersionAtOnce (idPropMeta, versionPropMeta)
      else
        Insert_GetIdentityAndVersionLater (idPropMeta, versionPropMeta)
    | IdentityId idPropMeta, _ ->
      if dialect.CanGetIdentityAtOnce then
        Insert_GetIentityAtOnce idPropMeta
      else
        Insert_GetIdentityLater idPropMeta
    | _, ComputedVersion versionPropMeta ->
      if dialect.CanGetVersionAtOnce then
        Insert_GetVersionAtOnce versionPropMeta
      else
        Insert_GetVersionLater versionPropMeta
    | _ ->
      Insert

  let private getUpdateCase (dialect:IDialect) = function
    | ComputedVersion versionPropMeta ->
      if dialect.CanGetVersionAtOnce then
        Update_GetVersionAtOnce versionPropMeta
      else
        Update_GetVersionLater versionPropMeta
    | IncremetedVersion versionPropMeta -> 
      Update_IncrementVersion versionPropMeta
    | _ ->
      Update

  let private handleTableAttr dialect (typ:Type) =
    match Attribute.GetCustomAttribute(typ, typeof<TableAttribute>) with 
    | :? TableAttribute as attr ->
      let tableName, enclosedTableName, simpleName = MetaHelper.getDbObjectName attr typ.Name dialect
      tableName, enclosedTableName, attr.IsEnclosed, simpleName
    | _ -> 
      let typeName = typ.Name
      typeName, dialect.EncloseIdentifier typeName, false, typeName

  let private makeInternal dialect (typ:Type) =
    let tableName, enclosedTableName, isEnclosed, simpleTableName = handleTableAttr dialect typ
    let makePropMeta = PropMeta.make simpleTableName dialect
    let propMetaList = 
      FSharpType.GetRecordFields(typ)
      |> Seq.mapi makePropMeta
      |> Seq.toList
    let idPropMetaList =
      propMetaList
      |> List.filter (fun propMeta -> match propMeta.PropCase with Id _ -> true | _ -> false)
      |> List.partition (fun propMeta -> match propMeta.PropCase with Id(Assigned) -> true | _ -> false)
      |> function
        | [], [] -> List.empty
        | [], [idMeta] -> [idMeta]
        | [], _ -> raise <| MetaException(SR.TRANQ3002 (typ.FullName))
        | idMetaList, [] -> idMetaList
        | _, _ -> raise <| MetaException(SR.TRANQ3003 (typ.FullName))
    let versionPropMeta = 
      propMetaList
      |> List.filter (fun propMeta -> match propMeta.PropCase with Version _ -> true | _ -> false)
      |> function 
        | [] -> None
        | [versionMeta] -> Some versionMeta
        | _ -> raise <| MetaException(SR.TRANQ3004 (typ.FullName))
    let preInsertCase = getPreInsertCase dialect (idPropMetaList, versionPropMeta)
    let insertCase = getInsertCase dialect (idPropMetaList, versionPropMeta)
    let updateCase = getUpdateCase dialect versionPropMeta
    { EntityName = typ.Name
      TableName = tableName
      SqlTableName = if isEnclosed then enclosedTableName else tableName
      PropMetaList = propMetaList 
      IdPropMetaList = idPropMetaList
      VersionPropMeta = versionPropMeta
      Type = typ
      PreInsertCase = preInsertCase
      InsertCase = insertCase
      UpdateCase = updateCase
      MakeEntity = FSharpValue.PreComputeRecordConstructor typ }

  let private cache = ConcurrentDictionary<Type * string, Lazy<EntityMeta>>()

  let make typ (dialect: IDialect) =
    cache.GetOrAdd((typ, dialect.Name), Lazy(fun () -> makeInternal dialect typ))
    |> Lazy.force

[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module TupleMeta =

  let private makeInternal dialect typ = 
    let elements = FSharpType.GetTupleElements(typ)
    let (entityTypeAry, basicTypeAry) =
      elements
      |> Array.mapi (fun i typ -> (i, typ))
      |> Array.partition (fun (i, typ) -> Type.isRecord typ)
    let basicElmtMetaAry =
      basicTypeAry
      |> Array.map (fun (i, typ) -> { BasicElementMeta.Index = i; Type = typ })
    let entityElmtMetaAry = 
      entityTypeAry
      |> Array.map (fun (i, typ) -> { EntityElementMeta.Index = i; EntityMeta = EntityMeta.make typ dialect })
    if not <| Array.isEmpty entityElmtMetaAry && not <| Array.isEmpty basicElmtMetaAry then
      let basicLastIndex = Array.max (basicElmtMetaAry |> Array.map (fun elmt -> elmt.Index))
      let entityFirstIndex = Array.min (entityElmtMetaAry |> Array.map (fun elmt -> elmt.Index))
      if basicLastIndex > entityFirstIndex then
        let basicTypeName = elements.[basicLastIndex].FullName
        let entityTypeName = elements.[entityFirstIndex].FullName
        raise <| MetaException (SR.TRANQ3000 (entityTypeName, basicTypeName, typ.FullName))
    { BasicElementMetaList = Array.toList basicElmtMetaAry
      EntityElementMetaList = Array.toList entityElmtMetaAry
      Type = typ
      MakeTuple = FSharpValue.PreComputeTupleConstructor typ }

  let private cache = ConcurrentDictionary<Type * string, Lazy<TupleMeta>>()
  
  let make typ (dialect: IDialect) =
    cache.GetOrAdd((typ, dialect.Name), Lazy(fun () -> makeInternal dialect typ))
    |> Lazy.force

[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module ProcedureParamMeta =

  let private (|FSharpList|_|) (typ:Type) =
    if typ.IsGenericType && typ.GetGenericTypeDefinition() = typedefof<list<_>> then
      Some <| typ.GetGenericArguments().[0]
    else 
      None

  let private handleProcedureParamAttr (prop: PropertyInfo) =
    match Attribute.GetCustomAttribute(prop, typeof<ProcedureParamAttribute>) with 
    | :? ProcedureParamAttribute as attr ->
      let name = if attr.Name <> null then attr.Name else prop.Name
      name, attr.Direction, attr.SizeOpt, attr.PrecisionOpt, attr.ScaleOpt, attr.UdtTypeName
    | _ -> 
        prop.Name, Direction.Input, None, None, None, null

  let private getParamMetaCase encloser (prop:PropertyInfo) direction =
    let getElementCase elementType =
      if Type.isRecord elementType then
        EntityType (EntityMeta.make elementType encloser)
      else 
        TupleType (TupleMeta.make elementType encloser)
    let typ = prop.PropertyType
    if typ = typeof<Unit> then 
      Unit 
    else
      match direction with
      | Direction.Input -> Input
      | Direction.InputOutput -> InputOutput
      | Direction.Output -> Output
      | Direction.ReturnValue -> ReturnValue
      | Direction.Result ->
        match typ with
        | FSharpList elementType ->
          Result((getElementCase elementType), (Seq.changeToList elementType))
        | _ ->
          raise <| MetaException(SR.TRANQ3006 (typ.FullName, prop.Name))
      | _ -> failwith "unreachable."

  let make encloser index prop =
    let paramName, direction, size, precision, scale, udtTypeName = handleProcedureParamAttr prop
    let paramMetaCase = getParamMetaCase encloser prop direction
    { Index = index
      ParamName = paramName
      Type = prop.PropertyType
      ParamMetaCase = paramMetaCase
      Size = size
      Precision = precision
      Scale = scale
      UdtTypeName = udtTypeName
      Property = prop
      GetValue = FSharpValue.PreComputeRecordFieldReader prop }

[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module ProcedureMeta =

  let private handleProcedureAttr dialect (typ:Type) =
    match Attribute.GetCustomAttribute(typ, typeof<ProcedureAttribute>) with 
    | :? ProcedureAttribute as attr ->
      let procedureName, enclosedProcedureName, _ = MetaHelper.getDbObjectName attr typ.Name dialect
      procedureName, enclosedProcedureName, attr.IsEnclosed
    | _ -> 
      typ.Name, dialect.EncloseIdentifier typ.Name, false

  let private makeInternal dialect (typ:Type) = 
    let procedureName, enclosedProcedureName, isEnclosed = handleProcedureAttr dialect typ
    let procedureParamMetaList = 
      FSharpType.GetRecordFields(typ)
      |> Seq.mapi (fun i field -> ProcedureParamMeta.make dialect i field)
      |> Seq.toList
    procedureParamMetaList
      |> Seq.filter (fun p -> match p.ParamMetaCase with ReturnValue -> true | _ -> false )
      |> Seq.length
      |> fun returnValueCount -> 
         if returnValueCount > 1 then 
           raise <| MetaException(SR.TRANQ3008 typ.FullName)
    { ProcedureName = procedureName
      SqlProcedureName = if isEnclosed then enclosedProcedureName else procedureName
      ProcedureParamMetaList = procedureParamMetaList
      MakeProcedure = FSharpValue.PreComputeRecordConstructor typ }

  let private cache = ConcurrentDictionary<Type * string, Lazy<ProcedureMeta>>()

  let make typ (dialect: IDialect) =
    cache.GetOrAdd((typ, dialect.Name), Lazy(fun () -> makeInternal dialect typ))
    |> Lazy.force