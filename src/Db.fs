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
open System.Collections
open System.Collections.Concurrent
open System.Collections.Generic
open System.ComponentModel
open System.Data
open System.Data.Common
open System.Dynamic
open System.Runtime.InteropServices
open System.Text
open Microsoft.FSharp.Reflection
open Microsoft.FSharp.Text.Lexing

exception OptimisticLockError of PreparedStatement with
  override this.Message =
    match this :> exn with
    | OptimisticLockError(stmt) -> 
      let message = SR.TRANQ4013 (stmt.Text, stmt.Params)
      Message.format message
    | _ -> 
      Unchecked.defaultof<_>

exception UniqueConstraintError of PreparedStatement * string * exn with
  override this.Message =
    match this :> exn with
    | UniqueConstraintError(stmt, message, _) -> 
      let message = SR.TRANQ4014 (message, stmt.Text, stmt.Params)
      Message.format message
    | _ -> 
      Unchecked.defaultof<_>

type DbException (message, ?innerException:exn) =
  inherit InvalidOperationException (Message.format message, match innerException with Some ex -> ex | _ -> null)
  member this.MessageId = message.Id

module internal DbHelper =

  let convertFromDbToClr (dialect: IDialect) dbValue destType udtTypeName prop exnHandler =
    try
      dialect.ConvertFromDbToClr(dbValue, destType, udtTypeName, prop)
    with
    | exn ->
      exnHandler exn

  let convertFromColumnToProp dialect (propMeta:PropMeta) (dbValue:obj) =
    convertFromDbToClr dialect dbValue propMeta.Type null propMeta.Property (fun exn ->
        let typ = if dbValue = null then typeof<obj> else dbValue.GetType()
        raise <| DbException(SR.TRANQ4017(typ.FullName, propMeta.ColumnName, propMeta.Type.FullName, propMeta.PropName), exn) )

  let createColumnIndexes (reader:DbDataReader) =
    let length = reader.FieldCount
    let columnIndexes = Dictionary<string, ResizeArray<int>>(length, StringComparer.InvariantCultureIgnoreCase) :> IDictionary<string, ResizeArray<int>>
    for i in 0 .. length - 1 do
      let name = reader.GetName(i)
      match columnIndexes.TryGetValue(name) with
      | true, indexes ->
        indexes.Add(i) |> ignore;
      | _ ->
        let indexes = new ResizeArray<int>()
        indexes.Add(i) |> ignore
        columnIndexes.[name] <- indexes
    let result = Dictionary<string, int>(length, StringComparer.InvariantCultureIgnoreCase) :> IDictionary<string, int>
    columnIndexes |> Seq.iter (fun (KeyValue(name, indexes)) -> 
      indexes |> Seq.iteri (fun i columnIndex -> 
        let uniqueName = if i = 0 then name else name + string i
        result.[uniqueName] <- columnIndex))
    result

  let createPropMappings (entityMeta:EntityMeta) (columnIndexes:IDictionary<string, int>) =
    let propMappings = Array.zeroCreate entityMeta.PropMetaList.Length
    entityMeta.PropMetaList
    |> List.iteri (fun i propMeta -> 
      propMappings.[i] <-
        match columnIndexes.TryGetValue propMeta.ColumnName with
        | true, columnIndex -> propMeta, Some columnIndex
        | _ -> propMeta, None )
    propMappings

  let makeEntity<'T> (dialect: IDialect) (entityMeta:EntityMeta) (propMappings:(PropMeta * int option) array) (reader:DbDataReader) = 
    let propArray = Array.zeroCreate propMappings.Length
    propMappings 
    |> Array.iter (fun (propMeta, columnIndex) -> 
       let dbValue =
         match columnIndex with
         | Some columnIndex -> dialect.GetValue(reader, columnIndex, propMeta.Property)
         | _ -> Convert.DBNull
       propArray.[propMeta.Index] <- convertFromColumnToProp dialect propMeta dbValue)
    entityMeta.MakeEntity propArray

  let makeEntityList<'T> (dialect: IDialect) entityMeta reader = 
    let columnIndexes = createColumnIndexes reader
    let propMappings = createPropMappings entityMeta columnIndexes
    seq { 
      while reader.Read() do
        yield makeEntity dialect entityMeta propMappings reader }
    |> Seq.cast<'T>

  let makeTupleList<'T> dialect (tupleMeta:TupleMeta) (reader:DbDataReader) = 
    let fieldCount = reader.FieldCount
    if fieldCount < tupleMeta.BasicElementMetaList.Length then 
      raise <| DbException(SR.TRANQ4010())
    let columnIndexes = createColumnIndexes reader
    let entityMappings =
      tupleMeta.EntityElementMetaList 
      |> List.map (fun elMeta ->
        let propMappings = createPropMappings elMeta.EntityMeta columnIndexes
        elMeta, propMappings)
    let convertFromColumnToElement (elMeta:BasicElementMeta) (dbValue:obj) =
      convertFromDbToClr dialect dbValue elMeta.Type null null (fun exn ->
        let typ = if dbValue = null then typeof<obj> else dbValue.GetType()
        raise <| DbException(SR.TRANQ4018(typ.FullName, elMeta.Index, elMeta.Type.FullName, elMeta.Index), exn) )
    seq { 
      while reader.Read() do
        let tupleAry = Array.zeroCreate (tupleMeta.BasicElementMetaList.Length + tupleMeta.EntityElementMetaList.Length)
        tupleMeta.BasicElementMetaList
        |> Seq.map (fun elMeta -> elMeta, dialect.GetValue(reader, elMeta.Index, null))
        |> Seq.map (fun (elMeta, dbValue) -> elMeta, convertFromColumnToElement elMeta dbValue)
        |> Seq.iter (fun (elMeta, value) -> tupleAry.[elMeta.Index] <- value)
        for elMeta, propMappings in entityMappings do
          tupleAry.[elMeta.Index] <- makeEntity dialect elMeta.EntityMeta propMappings reader
        yield tupleMeta.MakeTuple(tupleAry) }
    |> Seq.cast<'T>

  let makeSingleList<'T> dialect typ (reader:DbDataReader) = 
    let convertFromColumnToReturn (dbValue:obj) =
      convertFromDbToClr dialect dbValue typ null null (fun exn ->
        let typ = if dbValue = null then typeof<obj> else dbValue.GetType()
        raise <| DbException(SR.TRANQ4019(typ.FullName, typ.FullName), exn) )
    seq { 
      while reader.Read() do
        let dbValue = dialect.GetValue(reader, 0, null)
        yield convertFromColumnToReturn dbValue }
    |> Seq.cast<'T>

  let remakeEntity<'T> (entity:'T, entityMeta:EntityMeta) propHandler =
    entityMeta.PropMetaList
    |> Seq.map (fun propMeta -> propMeta, propMeta.GetValue(upcast entity))
    |> Seq.map propHandler
    |> Seq.toArray
    |> entityMeta.MakeEntity :?> 'T

  let getEntityMeta<'T> dialect =
    EntityMeta.make typeof<'T> dialect

  let convertFromColumnToPropIfNecessary dialect (dbValueMap:Map<int, obj>) (propMeta:PropMeta, value) =
    match dbValueMap.TryFind propMeta.Index with
    | Some dbValue -> convertFromColumnToProp dialect propMeta dbValue
    | _ -> value

  let appendPreparedStatements stmt1 stmt2 =
    let text = stmt1.Text + "; " + stmt2.Text
    let formattedText = stmt1.FormattedText + "; " + stmt2.FormattedText
    let parameters = 
      List.append (stmt1.Params) (stmt2.Params)
    { Text = text; FormattedText = formattedText; Params = parameters } 

  let prepareVersionSelect (dialect: IDialect) entity (entityMeta:EntityMeta) (versionPropMeta:PropMeta) =
    let idMetaList = 
      entityMeta.IdPropMetaList
      |> List.map (fun propMeta -> 
        propMeta.ColumnName, propMeta.GetValue(entity), propMeta.Type)
    dialect.PrepareVersionSelect(entityMeta.TableName, versionPropMeta.ColumnName, idMetaList)

  let initVersionValue (dialect: IDialect) value typ =
    let init v t compose decompose =
      if not <| Type.isNumber t then
        raise <| DbException(SR.TRANQ4030 t.FullName)
      if v = box null || Number.lessThan(decompose v, 1) then 
        Number.one t |> compose |> Some
      else 
        None
    let conv v t =
      match dialect.DataConvRegistry.TryGet(t) with
      | Some(basicType, compose, decompose) ->
        if Type.isOption basicType then
          let element, elementType = Option.getElement basicType (decompose v)
          init element elementType (Option.make basicType) id
          |> Option.map compose
        else
          init v basicType compose decompose
      | _ -> init v t id id
    if Type.isOption typ then
      let element, elementType = Option.getElement typ value 
      conv element elementType
      |> Option.map (Option.make typ)
    else 
      conv value typ

  let incrVersionValue (dialect: IDialect) value typ =
    let incr v t compose decompose =
      if not <| Type.isNumber t then
        raise <| DbException(SR.TRANQ4029 t.FullName)
      let num =
        if v = box null then
          Number.one t
        else 
          Number.incr (decompose v)
      compose num
    let conv v t =
      match dialect.DataConvRegistry.TryGet(t) with
      | Some(basicType, compose, decompose) ->
        if Type.isOption basicType then
          let element, elementType = Option.getElement basicType (decompose v)
          incr element elementType ((Option.make basicType) >> compose) id
        else
          incr v basicType compose decompose
      | _ -> incr v t id id
    if Type.isOption typ then
      let element, elementType = Option.getElement typ value
      let value = conv element elementType 
      Option.make typ value
    else 
      conv value typ

  let raiseTooManyAffectedRowsError stmt rows =
    raise <| DbException(SR.TRANQ4012 (rows, stmt.Text, stmt.Params))

  let raiseNoAffectedRowError {Text = text; Params = parameters} =
    raise <| DbException(SR.TRANQ4011 (text, parameters))

  let makeEntityNotFoundError {Text = text; Params = paramerters} =
    DbException(SR.TRANQ4015(text, paramerters))

  let setupCommand {Config = {Dialect = dialect}; Transaction = tx} (stmt:PreparedStatement) (command:DbCommand) =
    Option.iter (fun tx -> command.Transaction <- tx) tx
    command.CommandText <- stmt.Text
    stmt.Params
    |> List.iter (fun param ->
      let dbParam = command.CreateParameter()
      dialect.SetupDbParam(param, dbParam)
      command.Parameters.Add dbParam |> ignore )
    dialect.MakeParamDisposer command

module internal Exec =

  let execute ({Config = {Dialect = dialect; Listener = listener}; Connection = con; Transaction = tx} as ctx) stmt commandHandler = 
    con.ConfirmOpen()
    use command = con.CreateCommand()
    use paramsDisposer = DbHelper.setupCommand ctx stmt command
    let txId = Option.map (fun tx -> tx.GetHashCode()) tx
    listener (Sql (txId, stmt))
    try
      commandHandler command
    with ex -> 
      if dialect.IsUniqueConstraintViolation(ex) then
        raise <| UniqueConstraintError (stmt, ex.Message, ex)
      else 
        reraise()

  let executeNonQuery ({Config = {Dialect = dialect}} as ctx) stmt =
    execute ctx stmt (fun command -> command.ExecuteNonQuery())

  let executeScalar ctx stmt =
    execute ctx stmt (fun command -> command.ExecuteScalar())
  
  let executeReader<'T> ({Config = {Dialect = dialect}} as ctx) stmt (readerHandler: DbDataReader -> 'T seq) = 
    execute ctx stmt (fun command -> 
      use reader = command.ExecuteReader()
      if not dialect.IsHasRowsPropertySupported || reader.HasRows then
        readerHandler reader |> Seq.toList
      else
        List.empty)

  let executeReaderWitUserHandler<'T> ({Config = {Dialect = dialect}} as ctx) stmt (readerHandler: DbDataReader -> 'T) =
    execute ctx stmt (fun command ->
      use reader = command.ExecuteReader()
      readerHandler reader)

  let executeAndGetFirst<'T> ctx stmt (readerHandler: DbDataReader -> 'T seq) =
    executeReader<'T> ctx stmt (fun reader -> Seq.truncate 1 (readerHandler reader))
    |> Seq.toList
    |> function 
      | [] -> DbHelper.raiseNoAffectedRowError stmt
      | h :: _ -> h

  let executeAndGetVersionAtOnce ctx dialect entity (entityMeta:EntityMeta) (versionPropMeta:PropMeta) stmt =
    let versionStmt = DbHelper.prepareVersionSelect dialect entity entityMeta versionPropMeta
    let stmt = DbHelper.appendPreparedStatements stmt versionStmt
    executeAndGetFirst<_> ctx stmt <| fun reader ->
      seq { while reader.Read() do yield dialect.GetValue(reader, 0, versionPropMeta.Property) }

  let getVersionOnly ctx dialect entity (entityMeta:EntityMeta) (versionPropMeta:PropMeta) =
    let stmt = DbHelper.prepareVersionSelect dialect entity entityMeta versionPropMeta
    executeAndGetFirst<_> ctx stmt <| fun reader ->
      seq { while reader.Read() do yield dialect.GetValue(reader, 0, versionPropMeta.Property) }

module internal ExecDifferred =

  let execute ({Config = {Dialect = dialect; Listener = listener}; Connection = con; Transaction = tx} as ctx) stmt commandHandler = 
    seq {
      con.ConfirmOpen()
      use command = con.CreateCommand()
      use paramsDisposer = DbHelper.setupCommand ctx stmt command
      let txId = Option.map (fun tx -> tx.GetHashCode()) tx
      listener (Sql (txId, stmt))
      yield! commandHandler command }

  let executeReader<'T> ({Config = {Dialect = dialect}} as ctx) stmt (readerHandler: DbDataReader -> 'T seq) = 
    execute ctx stmt (fun command -> seq { 
      use reader = command.ExecuteReader()
      if not dialect.IsHasRowsPropertySupported || reader.HasRows then
        yield! readerHandler reader
      else
        yield! Seq.empty })

[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module internal Script =

  let internal getReaderHandler<'T> dialect =
    let typ = typeof<'T>
    if Type.isRecord typ then
      let meta = EntityMeta.make typ dialect
      fun reader -> DbHelper.makeEntityList<'T> dialect meta reader
    elif Type.isTuple typ then
      let meta = TupleMeta.make typ dialect
      fun reader -> DbHelper.makeTupleList<'T> dialect meta reader
    elif Type.isBasic typ then
      fun reader -> DbHelper.makeSingleList<'T> dialect typ reader
    else
      raise <| DbException(SR.TRANQ4028())

  let query<'T> ({Config = {Dialect = dialect}} as ctx) sql parameters = 
    let readerHandler = getReaderHandler<'T> dialect
    let stmt = Sql.prepare dialect sql parameters
    Exec.executeReader<'T> ctx stmt readerHandler

  let paginate<'T> ({Config = {Dialect = dialect}} as ctx) sql parameters range = 
    let readerHandler = getReaderHandler<'T> dialect
    let stmt = Sql.preparePaginate dialect sql parameters range
    Exec.executeReader<'T> ctx stmt readerHandler

  let paginateAndCount<'T> ({Config = {Dialect = dialect}} as ctx) sql parameters range = 
    let readerHandler = getReaderHandler<'T> dialect
    let pagenageStmt, countStmt = Sql.preparePaginateAndCount dialect sql parameters range
    let rows = Exec.executeReader<'T> ctx pagenageStmt readerHandler
    let count = Exec.executeScalar ctx countStmt
    rows, Convert.ChangeType(count, typeof<int64>) :?> int64

  let iterate<'T> ({Config = {Dialect = dialect}} as ctx) sql parameters range handler = 
    let readerHandler = getReaderHandler<'T> dialect
    let stmt = Sql.preparePaginate dialect sql parameters range
    let source = ExecDifferred.executeReader<'T> ctx stmt readerHandler
    use ie = source.GetEnumerator()
    let next = ref true
    while !next && ie.MoveNext() do
      next := handler ie.Current

  let execute ({Config = {Dialect = dialect}} as ctx) sql parameters  = 
    let stmt = Sql.prepare dialect sql parameters
    Exec.executeNonQuery ctx stmt

  let executeReader<'T> ({Config = {Dialect = dialect}} as ctx) sql parameters readerHandler = 
    let stmt = Sql.prepare dialect sql parameters
    Exec.executeReaderWitUserHandler<'T> ctx stmt readerHandler

[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module internal Auto =

  type FindResult<'T> = Found of 'T | NotFound of PreparedStatement

  let private validateType<'T> =
    let typ = typeof<'T>
    if not <| Type.isRecord typ then
      raise <| DbException(SR.TRANQ4002 typ.FullName)

  let private get<'T when 'T : not struct> ({Dialect = dialect}) idList  = 
    if List.isEmpty idList then
      raise <| DbException(SR.TRANQ4004 ())
    let readerHandler, entityMeta = 
      let typ = typeof<'T>
      let entityMeta = EntityMeta.make typ dialect
      if entityMeta.IdPropMetaList.IsEmpty then
        raise <| DbException(SR.TRANQ4005 (typ.FullName))
      elif entityMeta.IdPropMetaList.Length <> idList.Length then
        raise <| DbException(SR.TRANQ4003 (entityMeta.IdPropMetaList.Length, idList.Length))
      DbHelper.makeEntityList<'T> dialect entityMeta, entityMeta
    let stmt = Sql.prepareFind dialect idList entityMeta
    stmt, readerHandler, entityMeta

  let tryFind<'T when 'T : not struct> ({Config = config} as ctx) idList = 
    validateType<'T>
    let stmt, readerHandler, entityMeta = get<'T> config idList
    match Exec.executeReader ctx stmt readerHandler with
    | [] -> NotFound stmt
    | entity :: [] -> Found entity
    | _ -> raise <| DbException(SR.TRANQ4016 (stmt.Text, stmt.Params)) 

  let private validateOptimisticLock version entity (versionPropMeta:PropMeta option) stmt =
    match versionPropMeta with
    | Some versionPropMeta ->
      let actualVersion = versionPropMeta.GetValue (upcast entity)
      if actualVersion = null || not <| actualVersion.Equals(version) then
        raise <| OptimisticLockError stmt
    | _ -> 
      raise <| OptimisticLockError stmt

  let tryFindWithVersion<'T when 'T : not struct> ({Config = config} as ctx) idList version = 
    validateType<'T>
    let stmt, readerHandler, entityMeta = get<'T> config idList
    match Exec.executeReader<'T> ctx stmt readerHandler with
    | [] -> NotFound stmt
    | entity :: [] -> 
      validateOptimisticLock version entity entityMeta.VersionPropMeta stmt
      Found entity
    | _ -> raise <| DbException(SR.TRANQ4016 (stmt.Text, stmt.Params)) 

  let private preInsert<'T> ({Config = {Dialect = dialect}} as ctx) (entity:'T) (entityMeta:EntityMeta) =
    let contextKey = ctx.Connection.ConnectionString
    let (|Sequence|_|) = function
      | GetSequenceAndInitVersion(idPropMeta, sequenceMeta, _)
      | GetSequence(idPropMeta, sequenceMeta) -> 
        let stmt = dialect.PrepareSequenceSelect (sequenceMeta.SqlSequenceName)
        let dbValue = sequenceMeta.Generate contextKey (fun () -> Exec.executeScalar ctx stmt)
        let value = DbHelper.convertFromColumnToProp dialect idPropMeta dbValue
        Some (value, idPropMeta)
      | _ -> None
    let (|Version|_|) = function
      | GetSequenceAndInitVersion(_, _, versionPropMeta)
      | InitVersion(versionPropMeta) -> 
        let value = versionPropMeta.GetValue (upcast entity)
        let typ = versionPropMeta.Type
        match DbHelper.initVersionValue dialect value typ with
        | Some value -> Some(value, versionPropMeta) 
        | _ -> None
      | _ -> None
    match entityMeta.PreInsertCase with
    | Some preInsertCase -> 
      match preInsertCase with
      | Sequence(idValue, idPropMeta) & Version(versionValue, versionPropMeta) -> 
        DbHelper.remakeEntity<'T> (entity, entityMeta) (fun (propMeta:PropMeta, value) ->
          if propMeta.Index = idPropMeta.Index then idValue
          elif propMeta.Index = versionPropMeta.Index then versionValue
          else value )
      | Sequence(idValue, idPropMeta) -> 
        DbHelper.remakeEntity<'T> (entity, entityMeta) (fun (propMeta:PropMeta, value) ->
          if propMeta.Index = idPropMeta.Index then idValue
          else value )
      | Version(versionValue, versionPropMeta) -> 
        DbHelper.remakeEntity<'T> (entity, entityMeta) (fun (propMeta:PropMeta, value) ->
          if propMeta.Index = versionPropMeta.Index then versionValue
          else value )
      | _ -> entity
    | _ -> entity

  let insert<'T when 'T : not struct> ({Config = {Dialect = dialect}} as ctx) (entity:'T) (opt:InsertOpt) =
    validateType<'T>
    let entityMeta = DbHelper.getEntityMeta<'T> dialect
    let entity = preInsert<'T> ctx entity entityMeta
    let stmt = Sql.prepareInsert dialect entity entityMeta opt
    let makeEntity dbValueMap =
      DbHelper.remakeEntity<'T> (entity, entityMeta) (DbHelper.convertFromColumnToPropIfNecessary dialect dbValueMap)
    let insert() =
      let rows = Exec.executeNonQuery ctx stmt
      if rows < 1 then 
        DbHelper.raiseNoAffectedRowError stmt
      elif 1 < rows then
        DbHelper.raiseTooManyAffectedRowsError stmt rows
    match entityMeta.InsertCase with
    | Insert_GetIdentityAndVersionAtOnce(idPropMeta, versionPropMeta) -> 
      let identityStmt = 
        dialect.PrepareIdentityAndVersionSelect(entityMeta.TableName, idPropMeta.ColumnName, versionPropMeta.ColumnName)
      let stmt = DbHelper.appendPreparedStatements stmt identityStmt
      let readerHandler (reader:DbDataReader) =
        seq { while reader.Read() 
                do yield dialect.GetValue(reader, 0, idPropMeta.Property), 
                         dialect.GetValue(reader, 1, versionPropMeta.Property) }
      let idValue, versionValue = Exec.executeAndGetFirst<_> ctx stmt readerHandler
      makeEntity <| Map.ofList [idPropMeta.Index, idValue; versionPropMeta.Index, versionValue]
    | Insert_GetIentityAtOnce(idPropMeta) ->
      let identityStmt = dialect.PrepareIdentitySelect(entityMeta.TableName, idPropMeta.ColumnName)
      let stmt = DbHelper.appendPreparedStatements stmt identityStmt
      let readerHandler (reader:DbDataReader) =
        seq { while reader.Read() do yield dialect.GetValue(reader, 0, idPropMeta.Property) }
      let idValue = Exec.executeAndGetFirst ctx stmt readerHandler
      makeEntity <| Map.ofList [idPropMeta.Index, idValue]
    | Insert_GetVersionAtOnce(versionPropMeta) -> 
      let versionValue = Exec.executeAndGetVersionAtOnce ctx dialect entity entityMeta versionPropMeta stmt
      makeEntity <| Map.ofList [versionPropMeta.Index, versionValue]
    | Insert_GetIdentityAndVersionLater(idPropMeta, versionPropMeta) -> 
      insert()
      let stmt = dialect.PrepareIdentityAndVersionSelect(entityMeta.TableName, idPropMeta.ColumnName, versionPropMeta.ColumnName)
      let readerHandler (reader:DbDataReader) =
        seq { while reader.Read() 
                do yield dialect.GetValue(reader, 0, idPropMeta.Property), 
                         dialect.GetValue(reader, 1, versionPropMeta.Property) }
      let idValue, versionValue = id Exec.executeAndGetFirst ctx stmt readerHandler
      makeEntity <| Map.ofList [idPropMeta.Index, idValue; versionPropMeta.Index, versionValue]
    | Insert_GetIdentityLater(idPropMeta) ->
      insert()
      let stmt = dialect.PrepareIdentitySelect(entityMeta.TableName, idPropMeta.ColumnName)
      let readerHandler (reader:DbDataReader) =
        seq { while reader.Read() do yield dialect.GetValue(reader, 0, idPropMeta.Property) }
      let idValue = Exec.executeAndGetFirst ctx stmt readerHandler
      makeEntity <| Map.ofList [idPropMeta.Index, idValue]
    | Insert_GetVersionLater(versionPropMeta) ->
      insert()
      let versionValue = Exec.getVersionOnly ctx dialect entity entityMeta versionPropMeta
      makeEntity <| Map.ofList [versionPropMeta.Index, versionValue]
    | Insert ->
      insert()
      entity

  let update<'T when 'T : not struct> ({Config = {Dialect = dialect}} as ctx) (entity:'T) (opt:UpdateOpt) =
    validateType<'T>
    let entityMeta = DbHelper.getEntityMeta<'T> dialect
    if entityMeta.IdPropMetaList.IsEmpty then
      raise <| DbException(SR.TRANQ4005 (typeof<'T>.FullName))
    let stmt = Sql.prepareUpdate dialect entity entityMeta opt
    let update () =
      let rows = Exec.executeNonQuery ctx stmt
      if rows < 1 then
        if opt.IgnoreVersion || entityMeta.VersionPropMeta.IsNone then
          DbHelper.raiseNoAffectedRowError stmt
        else
          raise <| OptimisticLockError stmt
      if 1 < rows then 
        DbHelper.raiseTooManyAffectedRowsError stmt rows
    match entityMeta.UpdateCase with
    | Update_GetVersionAtOnce versionPropMeta ->
      let versionValue = Exec.executeAndGetVersionAtOnce ctx dialect entity entityMeta versionPropMeta stmt
      let dbValueMap = Map.ofList [versionPropMeta.Index, versionValue]
      DbHelper.remakeEntity<'T> (entity, entityMeta) (DbHelper.convertFromColumnToPropIfNecessary dialect dbValueMap)
    | Update_GetVersionLater versionPropMeta -> 
      update()
      let versionValue = Exec.getVersionOnly ctx dialect entity entityMeta versionPropMeta
      let dbValueMap = Map.ofList [versionPropMeta.Index, versionValue]
      DbHelper.remakeEntity<'T> (entity, entityMeta) (DbHelper.convertFromColumnToPropIfNecessary dialect dbValueMap)
    | Update_IncrementVersion versionPropMeta -> 
      update ()
      DbHelper.remakeEntity<'T> (entity, entityMeta) (fun (propMeta:PropMeta, value) ->
        if propMeta.Index = versionPropMeta.Index then
          DbHelper.incrVersionValue dialect value propMeta.Type
        else
          value)
    | Update ->
      update ()
      entity

  let delete<'T when 'T : not struct> ({Config = {Dialect = dialect}} as ctx) (entity:'T) (opt:DeleteOpt) =
    validateType<'T>
    let entityMeta = DbHelper.getEntityMeta<'T> dialect
    if entityMeta.IdPropMetaList.IsEmpty then
      raise <| DbException(SR.TRANQ4005 (typeof<'T>.FullName))
    let stmt = Sql.prepareDelete dialect entity entityMeta opt
    let rows = Exec.executeNonQuery ctx stmt
    if rows < 1 then 
      if opt.IgnoreVersion || entityMeta.VersionPropMeta.IsNone then
        DbHelper.raiseNoAffectedRowError stmt
      else 
        raise <| OptimisticLockError stmt
    if 1 < rows then 
      DbHelper.raiseTooManyAffectedRowsError stmt rows

  let call<'T when 'T : not struct> ({Config = {Dialect = dialect}} as ctx) (procedure:'T) =
    validateType<'T>
    let typ = typeof<'T>
    let procedureMeta = ProcedureMeta.make typ dialect
    let stmt = Sql.prepareCall dialect procedure procedureMeta
    let convertFromDbToClr dbValue (paramMeta:ProcedureParamMeta) =
      DbHelper.convertFromDbToClr dialect dbValue paramMeta.Type paramMeta.UdtTypeName paramMeta.Property (fun exn ->
        let typ = if dbValue = null then typeof<obj> else dbValue.GetType()
        raise <| DbException(SR.TRANQ4023(typ.FullName, paramMeta.ParamName, procedureMeta.ProcedureName, paramMeta.Type.FullName), exn) )
    Exec.execute ctx stmt (fun command ->
      command.CommandType <- CommandType.StoredProcedure
      let procedureAry = Array.zeroCreate (procedureMeta.ProcedureParamMetaList.Length)
      using (command.ExecuteReader()) (fun reader ->
        try
          procedureMeta.ProcedureParamMetaList
          |> Seq.fold (fun (hasNextResult, reader) paramMeta -> 
            match paramMeta.ParamMetaCase with
            | Result (elementCase, typeConverter) -> 
              if hasNextResult then
                let resultList =
                  match elementCase with
                  | EntityType entityMeta -> DbHelper.makeEntityList dialect entityMeta reader
                  | TupleType tupleMeta -> DbHelper.makeTupleList dialect tupleMeta reader
                procedureAry.[paramMeta.Index] <- typeConverter resultList
              reader.NextResult(), reader
            | _ ->
              hasNextResult, reader) (true, reader) 
          |> ignore
        finally
          try
            while reader.NextResult() do ()
          with _ -> () )
      procedureMeta.ProcedureParamMetaList
      |> Seq.filter (fun paramMeta -> 
        match paramMeta.ParamMetaCase with
        | Result _ -> false
        | _ -> true )
      |> Seq.iter (fun paramMeta -> 
        let paramName = dialect.CreateParamName paramMeta.ParamName
        let valueCase =
          if command.Parameters.Contains(paramName) then
            let value = command.Parameters.[paramName].Value
            match paramMeta.ParamMetaCase with
            | Unit  -> failwith "unreachable."
            | Input -> value
            | _ -> convertFromDbToClr value paramMeta
          else 
            null
        procedureAry.[paramMeta.Index] <- valueCase)
      procedureMeta.MakeProcedure procedureAry :?> 'T )

[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Db =
  
  let query<'T> sql parameters = Tx(fun ctx state -> 
    Guard.argNotNull (sql, "sql") 
    Guard.argNotNull (parameters, "parameters")
    let ret = Script.query<'T> ctx sql parameters
    Success ret, state)

  let paginate<'T> sql parameters range = Tx(fun ctx state -> 
    Guard.argNotNull (sql, "sql")
    Guard.argNotNull (parameters, "parameters")
    Guard.argNotNull (range, "range")
    let ret = Script.paginate<'T> ctx sql parameters range
    Success ret, state)

  let paginateAndCount<'T> sql parameters range = Tx(fun ctx state -> 
    Guard.argNotNull (sql, "sql")
    Guard.argNotNull (parameters, "parameters")
    Guard.argNotNull (range, "range")
    let ret = Script.paginateAndCount<'T> ctx sql parameters range
    Success ret, state)

  let iterate<'T> sql parameters range handler = Tx(fun ctx state -> 
    Guard.argNotNull (sql, "sql")
    Guard.argNotNull (parameters, "parameters")
    Guard.argNotNull (range, "range")
    let ret = Script.iterate<'T> ctx sql parameters range handler
    Success ret, state)

  let execute sql parameters = Tx(fun ctx state -> 
    Guard.argNotNull (sql, "sql")
    Guard.argNotNull (parameters, "parameters")
    let ret = Script.execute ctx sql parameters 
    Success ret, state)

  let run sql parameters = Tx(fun ctx state -> 
    Guard.argNotNull (sql, "sql")
    Guard.argNotNull (parameters, "parameters")
    Script.execute ctx sql parameters |> ignore
    Success (), state)

  let executeReader<'T> sql parameters readerHandler = Tx(fun ctx state ->
    Guard.argNotNull (sql, "sql")
    Guard.argNotNull (parameters, "parameters")
    Guard.argNotNull (readerHandler, "readerHandler")
    let ret = Script.executeReader<'T> ctx sql parameters readerHandler
    Success ret, state)

  let find<'T when 'T : not struct> id = Tx(fun ctx state -> 
    Guard.argNotNull (id, "id")
    match Auto.tryFind<'T> ctx id with
    | Auto.Found value -> Success value, state
    | Auto.NotFound stmt -> Failure (DbHelper.makeEntityNotFoundError stmt), state)

  let tryFind<'T when 'T : not struct> id = Tx(fun ctx state -> 
    Guard.argNotNull (id, "id")
    match Auto.tryFind<'T> ctx id with
    | Auto.Found value -> 
      Success (Some value), state
    | Auto.NotFound _ -> 
      Success None, state)

  let findWithVersion<'T when 'T : not struct> id version = Tx(fun ctx state -> 
    Guard.argNotNull (id, "id")
    match Auto.tryFindWithVersion<'T> ctx id version with
    | Auto.Found value -> 
      Success value, state
    | Auto.NotFound stmt -> 
      Failure (DbHelper.makeEntityNotFoundError stmt), state)

  let tryFindWithVersion<'T when 'T : not struct> id version = Tx(fun ctx state -> 
    Guard.argNotNull (id, "id")
    match Auto.tryFindWithVersion<'T> ctx id version with
    | Auto.Found value -> 
      Success (Some value), state
    | Auto.NotFound _ -> 
      Success None, state)

  let insert<'T when 'T : not struct> (entity: 'T) = Tx(fun ctx state -> 
    Guard.argNotNull (entity, "entity")
    let ret = Auto.insert ctx entity (InsertOpt())
    Success ret, state)

  let insertWithOpt<'T when 'T : not struct> (entity: 'T) opt = Tx(fun ctx state -> 
    Guard.argNotNull (entity, "entity")
    Guard.argNotNull (opt, "opt") 
    let ret = Auto.insert ctx entity opt
    Success ret, state)

  let update<'T when 'T : not struct> (entity: 'T) = Tx(fun ctx state -> 
    Guard.argNotNull (entity, "entity")
    let ret = Auto.update ctx entity (UpdateOpt())
    Success ret, state)

  let updateWithOpt<'T when 'T : not struct> (entity: 'T) opt = Tx(fun ctx state -> 
    Guard.argNotNull (entity, "entity")
    Guard.argNotNull (opt, "opt") 
    let ret = Auto.update ctx entity opt
    Success ret, state)

  let delete<'T when 'T : not struct> (entity: 'T) = Tx(fun ctx state -> 
    Guard.argNotNull (entity, "entity")
    let ret = Auto.delete ctx entity (DeleteOpt())
    Success ret, state)

  let deleteWithOpt<'T when 'T : not struct> (entity: 'T) opt = Tx(fun ctx state -> 
    Guard.argNotNull (entity, "entity")
    Guard.argNotNull (opt, "opt") 
    let ret = Auto.delete ctx entity opt
    Success ret, state)

  let call<'T when 'T : not struct> (procedure: 'T) = Tx(fun ctx state -> 
    Guard.argNotNull (procedure, "procedure")
    let ret = Auto.call ctx procedure
    Success ret, state)

