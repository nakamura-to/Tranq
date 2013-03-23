#r @"..\..\packages\FSPowerPack.Core.Community.3.0.0.0\Lib\Net40\FSharp.PowerPack.dll"
#r @"lib\Tranq.dll"

open Tranq

/// configuration
let config =
  let provide() =
    let conStr = "Data Source=.\SQLEXPRESS;Initial Catalog=tempdb;Integrated Security=True;" 
    new System.Data.SqlClient.SqlConnection(conStr) :> System.Data.Common.DbConnection
  let log = function
    | SqlIssuing(_, stmt) -> printfn "LOG: %s" stmt.FormattedText
    |_ -> ()  
  { Dialect = MsSqlDialect(); ConnectionProvider =provide; Listener = log }

/// evals a transaction workflow
let eval workflow = Tx.eval config workflow
