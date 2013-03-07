open System.Data.Common
open System.Data.SqlClient
open Tranq

module Email =
  type t = Email of string | InvalidEmail of string
  let make v = if String.exists ((=) '@') v then Email v else InvalidEmail v
  let conv = { new IDataConv<t, string> with
    member this.Compose(value) = make value
    member this.Decompose(email) = match email with Email v | InvalidEmail v -> v }

module Age =
  type t = Age of int option
  let make = Age
  let conv = { new IDataConv<t, int option> with
    member this.Compose(value) = make value
    member this.Decompose(Age(value)) = value }
  let incr (Age(age)) =
    age |> Option.map ((+) 1) |> make

module Version =
  type t = Version of int
  let make = Version
  let conv = { new IDataConv<t, int> with
    member this.Compose(value) = make value
    member this.Decompose(Version(value)) = value }

module Person =
  [<Table(Name = "Person")>]
  type t = { [<Id>]Id: int; Name: string; Email: Email.t; Age: Age.t; [<Version>]Version: Version.t }
  let incrAge person = { person with Age = Age.incr person.Age }

module PersonDao =
  type t = Person.t

  let setup = txSupports { 
    do! Db.run "
        if exists (select * from dbo.sysobjects where id = object_id(N'Person')) drop table Person;
        create table Person (Id int primary key, Name varchar(20), Email varchar(50), Age int, Version int);
        " []
    do! Db.insert<t> { Id = 1; Name = "hoge"; Email = Email.make "hoge@example.com"; Age = Age.make <| Some 10; Version = Version.make -1 } |> Tx.ignore
    do! Db.insert<t> { Id = 2; Name = "foo"; Email = Email.make "foo_example.com"; Age = Age.make None; Version = Version.make -1 } |> Tx.ignore
    do! Db.insert<t> { Id = 3; Name = "bar"; Email = Email.make "bar@example.com"; Age = Age.make <| Some 20; Version = Version.make -1 } |> Tx.ignore }

  let queryAll () = txSupports {
    return! Db.query<t> "select * from Person" [] }

  let queryByEmail email = txSupports {
    return! Db.query<t> "select * from Person where Name = @name" ["@name" <-- (email: Email.t)] }

  let find (id: int) = txSupports {
    return! Db.find<t> [id] }

  let update person = txSupports {
    return! Db.update<t> person }

/// query, increment age and then update
let workflow1 = txRequired {
  do! PersonDao.setup
  let! persons = PersonDao.queryAll()
  return! persons |> List.map Person.incrAge |> Tx.mapM PersonDao.update }

// query by name and query by identifier
let workflow2 = txRequired {
  do! PersonDao.setup
  let! p1 = PersonDao.queryByEmail <| Email.make "hoge@example.com"
  let! p2 = PersonDao.find 2
  return p1 @ [p2] }

let config = 
  let dataConvRepo =
    let reg = DataConvRegistry()
    reg.Add(Email.conv)
    reg.Add(Age.conv)
    reg.Add(Version.conv)
    reg
  let connectionString = "Data Source=.\SQLEXPRESS;Initial Catalog=tempdb;Integrated Security=True;" 
  let log = printfn "LOG: %s"
  { Dialect = MsSqlDialect(dataConvRepo)
    ConnectionProvider = fun () -> new SqlConnection(connectionString) :> DbConnection
    Listener = function
      | TxBegin(txId, _, _)-> log (sprintf "txId=%d, tx begin" txId)
      | TxEnd(txId, _, _, commit)-> 
        let kind = if commit then "commit" else "rollback"
        log (sprintf "txId=%d tx %s" txId kind)
      | Sql(txId, stmt) -> log (sprintf "txId=%A, sql=[%s]" txId stmt.FormattedText) }

let eval tx =
  match Tx.eval config tx with
  | Success ret -> printfn "success: %A\n" ret
  | Failure exn -> printfn "failure: %A\n" exn

[<EntryPoint>]
let main argv = 
  printfn "------------- workflow1 -------------"
  eval workflow1
  printfn "------------- workflow2 -------------"
  eval workflow2
  System.Console.ReadKey() |> fun _ -> 0
