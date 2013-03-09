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

namespace Tranq.Test.MsSql

open System.Data.Common
open System.Data.SqlClient
open System.Text.RegularExpressions
open Tranq

module Runner =

  let config = 
    let connectionString = "Data Source=.\SQLEXPRESS;Initial Catalog=TranqTest;Integrated Security=True;" 
    let log text = stdout.WriteLine("LOG: " + text)
    let listener = function
      | TxBegin(txId, _, _)-> log (sprintf "txId=%d, tx begin" txId)
      | TxCommit(txId, _, _)-> log (sprintf "txId=%d, tx commit" txId)
      | TxRollback(txId, _, _)-> log (sprintf "txId=%d, tx rollback" txId)
      | Sql(txId, stmt) -> 
        let txId = match txId with Some v -> string v | _ -> ""
        log (sprintf "txId=%s, sql=[%s]" txId stmt.FormattedText)
    { Dialect = MsSqlDialect()
      ConnectionProvider = fun () -> new SqlConnection(connectionString) :> DbConnection
      Listener = listener }

  let rollbackOnly tx =
    let tx = txRequired {
      do! Tx.rollbackOnly
      return! tx }
    Tx.eval config tx

[<AutoOpen>]
module Exn =

  let messageId (exn: exn) =
    let m = Regex.Match(exn.Message, @"^\[([^\]]*)\].*")
    if m.Success then
      m.Groups.[1].Value
    else 
      ""

[<AutoOpen>]
module Assert =

  let isEqualTo expected actual =
    if expected <> actual then
      failwithf "expected:\n%A\nactual:\n%A" expected actual

  let isTrue actual =
    if actual <> true then
      failwithf "expected: true"

  let isFalse actual =
    if actual <> false then
      failwithf "expected: false"