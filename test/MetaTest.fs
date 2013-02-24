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

module EntityMetaTest =

  open System
  open NUnit.Framework
  open Tranq
  open TestTool

  let dialect = MsSqlDialect() :> IDialect

  type Hoge1 = { Id:int }

  [<Test>]
  let ``make : propMeta`` () =
    let meta = EntityMeta.make typeof<Hoge1> dialect
    let propMeta = meta.PropMetaList.[0]
    assert_equal "Id" propMeta.PropName
    assert_equal "Id" propMeta.ColumnName
    assert_equal true propMeta.IsInsertable
    assert_equal true propMeta.IsUpdatable
    assert_equal Basic propMeta.PropCase
    assert_equal typeof<int> propMeta.Type
    assert_equal 10 (propMeta.GetValue (box { Hoge1.Id = 10 }))

  type Hoge2 = { [<Id>]Id:int }

  [<Test>]
  let ``make : propMeta : IdAttribute`` () =
    let meta = EntityMeta.make typeof<Hoge2> dialect
    let propMeta = meta.PropMetaList.[0]
    assert_equal "Id" propMeta.PropName
    assert_equal "Id" propMeta.ColumnName
    assert_equal true propMeta.IsInsertable
    assert_equal true propMeta.IsUpdatable
    assert_true  (propMeta.PropCase |> function Id Assigned -> true | _ -> false)
    assert_equal typeof<int> propMeta.Type
    assert_equal 10 (propMeta.GetValue (box { Hoge2.Id = 10 }))

  type Hoge3 = { [<Column(Name = "Aaa", Insertable = false, Updatable = false)>]Bbb:string }

  [<Test>]
  let ``make : propMeta : ColumnAttribute`` () =
    let meta = EntityMeta.make typeof<Hoge3> dialect
    let propMeta = meta.PropMetaList.[0]
    assert_equal "Bbb" propMeta.PropName
    assert_equal "Aaa" propMeta.ColumnName
    assert_equal false propMeta.IsInsertable
    assert_equal false propMeta.IsUpdatable
    assert_equal Basic propMeta.PropCase
    assert_equal typeof<string> propMeta.Type
    assert_equal "abc" (propMeta.GetValue (box { Hoge3.Bbb = "abc" }))

  type Hoge4 = { Name:string }

  [<Test>]
  let ``make : no id`` () =
    let meta = EntityMeta.make typeof<Hoge4> dialect
    assert_equal "Hoge4" meta.TableName
    assert_equal 1 meta.PropMetaList.Length
    assert_true meta.IdPropMetaList.IsEmpty
    assert_true meta.VersionPropMeta.IsNone

  type Hoge5 = { [<Id>]Id:int }

  [<Test>]
  let ``make : single id`` () =
    let meta = EntityMeta.make typeof<Hoge5> dialect
    assert_equal "Hoge5" meta.TableName
    assert_equal 1 meta.PropMetaList.Length
    assert_equal 1 meta.IdPropMetaList.Length
    assert_true meta.VersionPropMeta.IsNone

  type Hoge6 = { [<Id>]Id1:int; [<Id>]Id2:int }

  [<Test>]
  let ``make : multiple id`` () =
    let meta = EntityMeta.make typeof<Hoge6> dialect
    assert_equal "Hoge6" meta.TableName
    assert_equal 2 meta.PropMetaList.Length
    assert_equal 2 meta.IdPropMetaList.Length
    assert_true meta.VersionPropMeta.IsNone

  type Hoge7 = { [<Id>]Id:int; [<Version>]Version:int }

  [<Test>]
  let ``make : version`` () =
    let meta = EntityMeta.make typeof<Hoge7> dialect
    assert_equal "Hoge7" meta.TableName
    assert_equal 2 meta.PropMetaList.Length
    assert_equal 1 meta.IdPropMetaList.Length
    assert_true meta.VersionPropMeta.IsSome

  type Hoge14 = { [<Id(IdKind.Identity)>]Id1:int; [<Id(IdKind.Identity)>]Id2:int; [<Version>]Version:int }

  [<Test>]
  let ``make : multi non-assigned id`` () =
    try
      EntityMeta.make typeof<Hoge14> dialect |> ignore
      fail ()
    with
    | :? MetaException as ex -> 
      printfn "%A" ex
      assert_equal "TRANQ3002" ex.MessageId

  type Hoge15 = { [<Id>]Id1:int; [<Id(IdKind.Identity)>]Id2:int; [<Version>]Version:int }

  [<Test>]
  let ``make : assdigned id and non-assigned id`` () =
    try
      EntityMeta.make typeof<Hoge15> dialect |> ignore
      fail ()
    with
    | :? MetaException as ex -> 
      printfn "%A" ex
      assert_equal "TRANQ3003" ex.MessageId

  type Hoge16 = { [<Id>]Id:int; [<Version>]Version1:int; [<Version>]Version2:int }

  [<Test>]
  let ``make : multi version`` () =
    try
      EntityMeta.make typeof<Hoge16> dialect |> ignore
      fail ()
    with
    | :? MetaException as ex -> 
      printfn "%A" ex
      assert_equal "TRANQ3004" ex.MessageId

  type Hoge17 = { [<Id>][<Version>]Id:int; Name:string }

  [<Test>]
  let ``make : both id and version`` () =
    try
      EntityMeta.make typeof<Hoge17> dialect |> ignore
      fail ()
    with
    | :? MetaException as ex -> 
      printfn "%A" ex
      assert_equal "TRANQ3005" ex.MessageId

  [<Table(Catalog = "CATALOG")>]
  type Hoge8 = { [<Id>]Id:int; Name:string }

  [<Test>]
  let ``make : catalog`` () =
    let meta = EntityMeta.make typeof<Hoge8> dialect
    assert_equal "CATALOG.Hoge8" meta.TableName
    assert_equal "CATALOG.Hoge8" meta.SqlTableName
    assert_equal 2 meta.PropMetaList.Length
    assert_equal 1 meta.IdPropMetaList.Length
    assert_true meta.VersionPropMeta.IsNone

  [<Table(Catalog = "CATALOG", IsEnclosed = true)>]
  type Hoge30 = { [<Id>]Id:int; Name:string }

  [<Test>]
  let ``make : catalog isEnclosed`` () =
    let meta = EntityMeta.make typeof<Hoge30> dialect
    assert_equal "CATALOG.Hoge30" meta.TableName
    assert_equal "[CATALOG].[Hoge30]" meta.SqlTableName
    assert_equal 2 meta.PropMetaList.Length
    assert_equal 1 meta.IdPropMetaList.Length
    assert_true meta.VersionPropMeta.IsNone

  [<Table(Schema = "SCHEMA")>]
  type Hoge9 = { [<Id>]Id:int; Name:string }

  [<Test>]
  let ``make : schema`` () =
    let meta = EntityMeta.make typeof<Hoge9> dialect
    assert_equal "SCHEMA.Hoge9" meta.TableName
    assert_equal "SCHEMA.Hoge9" meta.SqlTableName

    assert_equal 2 meta.PropMetaList.Length
    assert_equal 1 meta.IdPropMetaList.Length
    assert_true meta.VersionPropMeta.IsNone

  [<Table(Schema = "SCHEMA", IsEnclosed = true)>]
  type Hoge31 = { [<Id>]Id:int; Name:string }

  [<Test>]
  let ``make : schema isEnclosed`` () =
    let meta = EntityMeta.make typeof<Hoge31> dialect
    assert_equal "SCHEMA.Hoge31" meta.TableName
    assert_equal "[SCHEMA].[Hoge31]" meta.SqlTableName
    assert_equal 2 meta.PropMetaList.Length
    assert_equal 1 meta.IdPropMetaList.Length
    assert_true meta.VersionPropMeta.IsNone

  [<Table(Name = "TABLE")>]
  type Hoge10 = { [<Id>]Id:int; Name:string }

  [<Test>]
  let ``make : table`` () =
    let meta = EntityMeta.make typeof<Hoge10> dialect
    assert_equal "TABLE" meta.TableName
    assert_equal "TABLE" meta.SqlTableName
    assert_equal 2 meta.PropMetaList.Length
    assert_equal 1 meta.IdPropMetaList.Length
    assert_true meta.VersionPropMeta.IsNone

  [<Table(Name = "TABLE", IsEnclosed = true)>]
  type Hoge32 = { [<Id>]Id:int; Name:string }

  [<Test>]
  let ``make : table isEnclosed`` () =
    let meta = EntityMeta.make typeof<Hoge32> dialect
    assert_equal "TABLE" meta.TableName
    assert_equal "[TABLE]" meta.SqlTableName
    assert_equal 2 meta.PropMetaList.Length
    assert_equal 1 meta.IdPropMetaList.Length
    assert_true meta.VersionPropMeta.IsNone

  [<Table(Catalog = "CATALOG", Schema = "SCHEMA", Name = "TABLE")>]
  type Hoge11 = { [<Id>]Id:int; Name:string }

  [<Test>]
  let ``make : catalog schema table`` () =
    let meta = EntityMeta.make typeof<Hoge11> dialect
    assert_equal "CATALOG.SCHEMA.TABLE" meta.TableName
    assert_equal "CATALOG.SCHEMA.TABLE" meta.SqlTableName
    assert_equal 2 meta.PropMetaList.Length
    assert_equal 1 meta.IdPropMetaList.Length
    assert_true meta.VersionPropMeta.IsNone

  [<Table(Catalog = "CATALOG", Schema = "SCHEMA", Name = "TABLE", IsEnclosed = true)>]
  type Hoge33 = { [<Id>]Id:int; Name:string }

  [<Test>]
  let ``make : catalog schema table isEnclosed`` () =
    let meta = EntityMeta.make typeof<Hoge33> dialect
    assert_equal "CATALOG.SCHEMA.TABLE" meta.TableName
    assert_equal "[CATALOG].[SCHEMA].[TABLE]" meta.SqlTableName
    assert_equal 2 meta.PropMetaList.Length
    assert_equal 1 meta.IdPropMetaList.Length
    assert_true meta.VersionPropMeta.IsNone

  type Hoge12 = { [<Id>][<Column(Name="pk")>]Id:int; [<Version>][<Column(Name="version")>]Version:int }

  [<Test>]
  let ``make : column`` () =
    let meta = EntityMeta.make typeof<Hoge12> dialect
    assert_equal "Hoge12" meta.TableName
    assert_equal 2 meta.PropMetaList.Length
    let pkPropMeta = meta.PropMetaList.[0]
    assert_equal "pk" pkPropMeta.ColumnName
    assert_equal "pk" pkPropMeta.SqlColumnName
    let versionPropMeta = meta.PropMetaList.[1]
    assert_equal "version" versionPropMeta.ColumnName
    assert_equal "version" versionPropMeta.SqlColumnName
    assert_equal 1 meta.IdPropMetaList.Length
    assert_true meta.VersionPropMeta.IsSome

  type Hoge34 = { [<Id>][<Column(Name="pk", IsEnclosed=true)>]Id:int; [<Version>][<Column(Name="version", IsEnclosed=true)>]Version:int }

  [<Test>]
  let ``make : column isEnclosed`` () =
    let meta = EntityMeta.make typeof<Hoge34> dialect
    assert_equal "Hoge34" meta.TableName
    assert_equal 2 meta.PropMetaList.Length
    let pkPropMeta = meta.PropMetaList.[0]
    assert_equal "pk" pkPropMeta.ColumnName
    assert_equal "[pk]" pkPropMeta.SqlColumnName
    let versionPropMeta = meta.PropMetaList.[1]
    assert_equal "version" versionPropMeta.ColumnName
    assert_equal "[version]" versionPropMeta.SqlColumnName
    assert_equal 1 meta.IdPropMetaList.Length
    assert_true meta.VersionPropMeta.IsSome

  [<Test>]
  let ``make`` () =
    let meta = EntityMeta.make typeof<Hoge1> dialect
    let meta2 = EntityMeta.make typeof<Hoge1> dialect
    assert_true (obj.ReferenceEquals(meta, meta2))

  type Hoge40 = { [<Id>]Id:int; [<Version>]Version:int; Name :string }

  [<Test>]
  let ``make : MakeEntity : record`` () =
    let meta = EntityMeta.make typeof<Hoge40> dialect
    let entity = meta.MakeEntity [| 1; 2; "aaa" |] :?> Hoge40
    assert_equal 1 entity.Id
    assert_equal 2 entity.Version
    assert_equal "aaa" entity.Name

  type Hoge60 = { [<Id(IdKind.Sequence)>][<Sequence>]Id:int; [<Version>]Version:int }

  [<Test>]
  let ``make : sequence `` () =
    let entityMeta = EntityMeta.make typeof<Hoge60> dialect
    let propertyMeta = entityMeta.IdPropMetaList.[0]
    match propertyMeta.PropCase with
    | Id (Sequence (sequenceMeta)) -> 
      assert_equal "Hoge60_SEQ" sequenceMeta.SequenceName
    | _ -> 
      fail ()

  type Hoge61 = { [<Id(IdKind.Sequence)>][<Sequence(Catalog = "CATALOG", Schema = "SCHEMA", Name = "SEQUENCE")>]Id:int; [<Version>]Version:int }

  [<Test>]
  let ``make : sequence : catalog schema name`` () =
    let entityMeta = EntityMeta.make typeof<Hoge61> dialect
    let propertyMeta = entityMeta.IdPropMetaList.[0]
    match propertyMeta.PropCase with
    | Id (Sequence (sequenceMeta)) -> 
      assert_equal "CATALOG.SCHEMA.SEQUENCE" sequenceMeta.SequenceName
    | _ -> 
      fail ()

  type Hoge62 = { [<Id(IdKind.Sequence)>]Id:int; [<Version>]Version:int }

  [<Test>]
  let ``make : sequence not found `` () =
    try 
      EntityMeta.make typeof<Hoge62> dialect |> ignore
      fail ()
    with
    | :? MetaException as ex -> 
      printfn "%s" ex.Message
      assert_equal "TRANQ3009" ex.MessageId
    | ex -> 
      fail ex

  [<Table(Name = "FOO")>]
  type Hoge63 = { [<Id(IdKind.Sequence)>][<Sequence>]Id:int; [<Version>]Version:int }

  [<Test>]
  let ``make : sequence : use tableName `` () =
    let entityMeta = EntityMeta.make typeof<Hoge63> dialect
    let propertyMeta = entityMeta.IdPropMetaList.[0]
    match propertyMeta.PropCase with
    | Id (Sequence (sequenceMeta)) -> 
      assert_equal "FOO_SEQ" sequenceMeta.SequenceName
    | _ -> 
      fail ()

module TupleMetaTest =

  open System
  open NUnit.Framework
  open Tranq
  open TestTool

  let dialect = MsSqlDialect() :> IDialect

  type Hoge12 = { [<Id>][<Column(Name="pk")>]Id:int; [<Version>][<Column(Name="version")>]Version:int }

  [<Test>]
  let ``make`` () =
    let meta = TupleMeta.make typeof<int * string * Hoge12> dialect
    assert_equal 2 meta.BasicElementMetaList.Length
    assert_equal 1 meta.EntityElementMetaList.Length

  [<Test>]
  let ``make : record index is invalid`` () =
    try
      TupleMeta.make typeof<Hoge12 * int> dialect |> ignore
      fail ()
    with 
    | :? MetaException as ex -> 
      printfn "%s" ex.Message
      assert_equal "TRANQ3000" ex.MessageId
    | ex -> 
      fail ex

module ProcedureMetaTest = 

  open System
  open NUnit.Framework
  open Tranq
  open TestTool

  let dialect = MsSqlDialect() :> IDialect

  type Hoge20 = { Param1 : int; Param2 : string }

  [<Test>]
  let ``make`` () =
    let meta = ProcedureMeta.make typeof<Hoge20> dialect
    assert_equal "Hoge20" meta.ProcedureName
    assert_equal "Hoge20" meta.SqlProcedureName
    assert_equal 2 meta.ProcedureParamMetaList.Length

  [<Procedure(IsEnclosed = true)>]
  type Hoge35 = { Param1 : int; Param2 : string }

  [<Test>]
  let ``make isEnclosed`` () =
    let meta = ProcedureMeta.make typeof<Hoge35> dialect
    assert_equal "Hoge35" meta.ProcedureName
    assert_equal "[Hoge35]" meta.SqlProcedureName
    assert_equal 2 meta.ProcedureParamMetaList.Length

  type Hoge21 = { Param1 : int; [<ProcedureParam(Direction = Direction.Result)>]Param2 : string }

  [<Test>]
  let ``make : illegal result type`` () =
    try
      ProcedureMeta.make typeof<Hoge21> dialect |> ignore
      fail ()
    with
    | :? MetaException as ex -> 
      printfn "%s" ex.Message
      assert_equal "TRANQ3006" ex.MessageId
    | ex -> 
      fail ex

  type Hoge22 = { [<ProcedureParam(Direction = Direction.ReturnValue)>]Param1 : int; [<ProcedureParam(Direction = Direction.ReturnValue)>]Param2 : string }

  [<Test>]
  let ``make : 2 ReturnValue parameters.`` () =
    try
      ProcedureMeta.make typeof<Hoge22> dialect |> ignore
      fail ()
    with
    | :? MetaException as ex -> 
      printfn "%s" ex.Message
      assert_equal "TRANQ3008" ex.MessageId
    | ex -> 
      fail ex

  type Hoge50 = { Param1:int; Param2:int; Param3:string }

  [<Test>]
  let ``make : MakeProcedure : record`` () =
    let meta = ProcedureMeta.make typeof<Hoge50> dialect
    let procedure = meta.MakeProcedure [| 1; 2; "aaa" |] :?> Hoge50
    assert_equal 1 procedure.Param1
    assert_equal 2 procedure.Param2
