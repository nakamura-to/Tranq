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
open Tranq.GlobalTx
open NUnit.Framework
open TestTool
open System.Data.Common
open System.Data.SqlClient

module GlobalTxTest =

  let config = 
    let provide() =new SqlConnection() :> DbConnection
    let listen event = ()
    { Dialect = MsSqlDialect(); ConnectionProvider = provide; Listener = listen }

  exception MyError
  
  [<Test>]
  let ``abort``() =
    let workflow = txSupports { 
      do! Tx.abort MyError
      failwith "not expected" }
    Tx.run config workflow
    |> function
    | Success ret, state -> failwith "not expected"
    | Failure ret, state -> 
      match ret with
      | MyError -> ()
      | _ -> failwith "not expected"

  [<Test>]
  let ``abortwith``() =
    let workflow = txSupports { 
      do! Tx.abortwith "hoge"
      failwith "not expected" }
    Tx.run config workflow
    |> function
    | Success ret, state -> failwith "not expected"
    | Failure ret, state -> 
      match ret with
      | Abort m -> m |> assert_equal "hoge"
      | _ -> failwith "not expected"

  [<Test>]
  let ``abortwithf``() =
    let workflow = txSupports { 
      do! Tx.abortwithf "%s %d" "hoge" 10
      failwith "not expected" }
    Tx.run config workflow
    |> function
    | Success ret, state -> failwith "not expected"
    | Failure ret, state -> 
      match ret with
      | Abort m -> m |> assert_equal "hoge 10"
      | _ -> failwith "not expected"

  [<Test>]
  let ``rollbackOnly``() =
    let workflow = txSupports {
      do! Tx.rollbackOnly
      return! Tx.isRollbackOnly }
    Tx.run config workflow
    |> function
    | Success ret, {IsRollbackOnly = isRollbackOnly} -> 
      assert_true ret
      assert_true isRollbackOnly
    | Failure ret, state -> failwith "not expected"

  [<Test>]
  let ``liftM``() =
    let workflow = txSupports {
      let m = Tx.returnM true
      return! Tx.liftM (not) m }
    Tx.run config workflow
    |> function
    | Success ret, _ ->
      assert_false ret
    | Failure ret, state -> failwith "not expected"

  [<Test>]
  let ``liftM: <!>``() =
    let workflow = txSupports {
      let m = Tx.returnM true
      return! (not) <!> m }
    Tx.run config workflow
    |> function
    | Success ret, _ ->
      assert_false ret
    | Failure ret, state -> failwith "not expected"

  [<Test>]
  let ``liftM2``() =
    let workflow = txSupports {
      let m1 = Tx.returnM 1
      let m2 = Tx.returnM 2
      return! Tx.liftM2 (+) m1 m2 }
    Tx.run config workflow
    |> function
    | Success ret, _ ->
      assert_equal 3 ret
    | Failure ret, state -> failwith "not expected"

  [<Test>]
  let ``applyM``() =
    let workflow = txSupports {
      let f = Tx.returnM not
      let m = Tx.returnM true
      return! Tx.applyM f m }
    Tx.run config workflow
    |> function
    | Success ret, _ ->
      assert_false ret
    | Failure ret, state -> failwith "not expected"

  [<Test>]
  let ``applyM : <*>``() =
    let workflow = txSupports {
      let f = Tx.returnM not
      let m = Tx.returnM true
      return! f <*> m }
    Tx.run config workflow
    |> function
    | Success ret, _ ->
      assert_false ret
    | Failure ret, state -> failwith "not expected"

  [<Test>]
  let ``Tx.ignore``() =
    let workflow = txSupports {
      let m = Tx.returnM 1
      do! m |> Tx.ignore }
    Tx.run config workflow
    |> function
    | Success ret, _ ->
      ()
    | Failure ret, state -> failwith "not expected"