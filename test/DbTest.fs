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

module DbHelperTest =

  type Version = Version of int

  type VersionOpt = VersionOpt of int option

  [<Test>]
  let ``initVersionValue: basic type``() =
    let dialect = MsSqlDialect()
    assert_equal (Some (box 1)) (DbHelper.initVersionValue dialect 0 typeof<int>)
    assert_equal None (DbHelper.initVersionValue dialect 10 typeof<int>)
    assert_equal (Some (box (Some 1))) (DbHelper.initVersionValue dialect None typeof<int option>)
    assert_equal None (DbHelper.initVersionValue dialect (Some 10) typeof<int option>)

  [<Test>]
  let ``initVersionValue: rich type``() =
    let repo = DataConvRepo()
    repo.Add({ new IDataConv<Version, int> with
      member this.Compose v = Version v
      member this.Decompose (Version(v)) = v})
    repo.Add({ new IDataConv<VersionOpt, int option> with
      member this.Compose v = VersionOpt v
      member this.Decompose (VersionOpt(v)) = v})
    let dialect = MsSqlDialect(repo)
    assert_equal (Some (box (Version 1))) (DbHelper.initVersionValue dialect (Version 0) typeof<Version>)
    assert_equal None (DbHelper.initVersionValue dialect (Version 10) typeof<Version>)
    assert_equal (Some (box (Some (Version 1)))) (DbHelper.initVersionValue dialect None typeof<Version option>)
    assert_equal None (DbHelper.initVersionValue dialect (Some (Version 10)) typeof<Version option>)
    assert_equal (Some (box (VersionOpt (Some 1)))) (DbHelper.initVersionValue dialect (VersionOpt None) typeof<VersionOpt>)
    assert_equal None (DbHelper.initVersionValue dialect (VersionOpt (Some 10)) typeof<VersionOpt>)

  [<Test>]
  let ``incrVersionValue: basic type``() =
    let dialect = MsSqlDialect()
    assert_equal (box 1) (DbHelper.incrVersionValue dialect 0 typeof<int>)
    assert_equal (box 11) (DbHelper.incrVersionValue dialect 10 typeof<int>)
    assert_equal (Some(1)) (DbHelper.incrVersionValue dialect None typeof<int option>)
    assert_equal (Some(11)) (DbHelper.incrVersionValue dialect (Some 10) typeof<int option>)

  [<Test>]
  let ``incrVersionValue: rich type``() =
    let repo = DataConvRepo()
    repo.Add({ new IDataConv<Version, int> with
      member this.Compose v = Version v
      member this.Decompose (Version(v)) = v})
    repo.Add({ new IDataConv<VersionOpt, int option> with
      member this.Compose v = VersionOpt v
      member this.Decompose (VersionOpt(v)) = v})
    let dialect = MsSqlDialect(repo)
    assert_equal (box (Version 1)) (DbHelper.incrVersionValue dialect (Version 0) typeof<Version>)
    assert_equal (box (Version 11)) (DbHelper.incrVersionValue dialect (Version 10) typeof<Version>)
    assert_equal (Some(Version 1)) (DbHelper.incrVersionValue dialect None typeof<Version option>)
    assert_equal (Some(Version 11)) (DbHelper.incrVersionValue dialect (Some (Version 10)) typeof<Version option>)
    assert_equal (box (VersionOpt (Some 1))) (DbHelper.incrVersionValue dialect (VersionOpt None) typeof<VersionOpt>)
    assert_equal (box (VersionOpt (Some 11))) (DbHelper.incrVersionValue dialect (VersionOpt (Some 10)) typeof<VersionOpt>)

module ScriptTest =

  let dialect = MsSqlDialect()

  type Record = { Name: string }

  [<Test>]
  let ``getReaderHandler``() =
    Script.getReaderHandler<Record> dialect |> ignore
    Script.getReaderHandler<string * int> dialect |> ignore
    Script.getReaderHandler<string> dialect |> ignore

  type Class() = class end

  [<Test>]
  let ``getReaderHandler: error``() =
    try
      Script.getReaderHandler<Class> dialect |> ignore
    with
    | :? DbException as ex -> 
      assert_equal "TRANQ4028" ex.MessageId
    | ex ->
      fail ex

module DbTest = 

  let dialect = MsSqlDialect()


//  [<Test>]
//  let ``paginate : order by not found`` () =
//    try
//      Db.paginateOnDemand<string> dialect "select * from Employee" [] (2L, 5L)  |> Seq.cache |> ignore
//      fail ()
//    with 
//    | :? SqlException as ex -> 
//      printfn "%s" ex.Message
//      assert_equal "SOMA2016" ex.MessageId
//    | ex -> 
//      fail ex

//  [<Test>]
//  let ``paginate : order by not found`` () =
//    try
//      Db.paginateOnDemand<string> dialect "select * from Employee" [] (2L, 5L)  |> Seq.cache |> ignore
//      fail ()
//    with 
//    | :? SqlException as ex -> 
//      printfn "%s" ex.Message
//      assert_equal "SOMA2016" ex.MessageId
//    | ex -> 
//      fail ex
//
//  [<Test>]
//  let ``find : generic parameter is invalid`` () =
//    try
//      Db.find<string> config [1] |> ignore
//      fail ()
//    with 
//    | :? DbException as ex -> 
//      printfn "%s" ex.Message
//      assert_equal "SOMA4002" ex.MessageId
//    | ex -> 
//      fail ex
//
//  type Hoge = {[<Id>]Id:int32; Name:string; [<Version>]Version:int32}
//
//  [<Test>]
//  let ``find : idList is empty`` () =
//    try
//      Db.find<Hoge> config [] |> ignore
//      fail ()
//    with 
//    | :? DbException as ex -> 
//      printfn "%s" ex.Message
//      assert_equal "SOMA4004" ex.MessageId
//    | ex -> 
//      fail ex
//
//  [<Test>]
//  let ``find : idList length is invalid`` () =
//    try
//      Db.find<Hoge> config [1; 2] |> ignore
//      fail ()
//    with 
//    | :? DbException as ex -> 
//      printfn "%s" ex.Message
//      assert_equal "SOMA4003" ex.MessageId
//    | ex -> 
//      fail ex
//
//  [<Test>]
//  let ``MsSqlConfig : object expression`` () =
//    let config = 
//      { new MsSqlConfig() with
//        member this.ConnectionString = "" }
//    ()
//
//  [<Test>]
//  let ``Conversion : toIdList`` () =
//    assert_equal [box "hoge"] (Conversion.toIdList "hoge")
//    assert_equal [box "hoge"; box "foo"] (Conversion.toIdList ["hoge"; "foo"])
//    assert_equal [box "hoge"; box 1] (Conversion.toIdList [box "hoge"; box 1])
//    assert_equal [box 1] (Conversion.toIdList 1)
//
//  [<Test>]
//  let ``Conversion : toExprCtxt : dict`` () =
//    let hash = Hashtable()
//    hash.["aaa"] <- "hoge"
//    hash.[100] <- 200
//    hash.["xxx"] <- null
//    let exprCtxt = Conversion.toExprCtxt hash
//    assert_equal (box "hoge", typeof<obj>) (exprCtxt.["aaa"])
//    assert_equal (box 200, typeof<obj>) (exprCtxt.["100"])
//    assert_equal (box null, typeof<obj>) (exprCtxt.["xxx"])
//
//  [<Test>]
//  let ``Conversion : toExprCtxt : list`` () =
//    let exprCtxt = Conversion.toExprCtxt ["aaa" @= "hoge"; "100" @= 200; "xxx" @= null]
//    assert_equal (box "hoge", typeof<string>) (exprCtxt.["aaa"])
//    assert_equal (box 200, typeof<int>) (exprCtxt.["100"])
//    assert_equal (box null, typeof<obj>) (exprCtxt.["xxx"])
//
//  [<Test>]
//  let ``Conversion : toExprCtxt : exception`` () =
//    try
//      Conversion.toExprCtxt "" |> ignore
//    with
//    | :? DbException as ex -> 
//      printfn "%s" ex.Message
//      assert_equal "SOMA4020" ex.MessageId
//    | ex -> 
//      fail ex