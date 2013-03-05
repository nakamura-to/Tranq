open System.Data.Common
open System.Data.SqlClient
open Tranq

module Email =
  type t = Email of string | InvalidEmail of string
  let create (v: string) = if v.Contains("@") then Email v else InvalidEmail v
  let conv = { new IDataConv<t, string> with
    member this.Compose(value) = create value
    member this.Decompose(email) = match email with Email v | InvalidEmail v -> v }

module Age =
  type t = Age of int option
  let create = Age
  let conv = { new IDataConv<t, int option> with
    member this.Compose(value) = create value
    member this.Decompose(Age(value)) = value }
  let incr (Age(age)) =
    age |> Option.map ((+) 1) |> create

module Version =
  type t = Version of int
  let create = Version
  let conv = { new IDataConv<t, int> with
    member this.Compose(value) = create value
    member this.Decompose(Version(value)) = value }

module Person =
  [<Table(Name = "Person")>]
  type t = { [<Id>]Id: int; Name: string; Email: Email.t; Age: Age.t; [<Version>]Version: Version.t }
  let incrAge person = { person with Age = Age.incr person.Age }

module Dao =
  let setup = txSupports { 
    do! Db.run "
        if exists (select * from dbo.sysobjects where id = object_id(N'Person')) drop table Person;
        create table Person (Id int primary key, Name varchar(20), Email varchar(50), Age int, Version int);
        " []
    do! Db.insert<Person.t> { Id = 1; Name = "hoge"; Email = Email.create "hoge@example.com"; Age = Age.create <| Some 10; Version = Version.create -1 } |> Tx.ignore
    do! Db.insert<Person.t> { Id = 2; Name = "foo"; Email = Email.create "foo_example.com"; Age = Age.create None; Version = Version.create -1 } |> Tx.ignore
    do! Db.insert<Person.t> { Id = 3; Name = "bar"; Email = Email.create "bar@example.com"; Age = Age.create <| Some 20; Version = Version.create -1 } |> Tx.ignore }

  let queryPersonAll () = txSupports {
    return! Db.query<Person.t> "select * from Person" [] }

  let queryPersonByName (name: string) = txSupports {
    return! Db.query<Person.t> "select * from Person where Name = @name" ["@name" <-- name]}

  let findPerson (id: int) = txSupports {
    return! Db.find<Person.t> [id] }

  let updatePerson person = txSupports {
    return! Db.update<Person.t> person }

/// query, increment Age values and then update
let workflow1 = txRequired {
  do! Dao.setup
  let! persons = Dao.queryPersonAll()
  return! persons |> List.map Person.incrAge |> Tx.mapM Dao.updatePerson }

// query by name and query by identifier
let workflow2 = txRequired {
  do! Dao.setup
  let! p1 = Dao.queryPersonByName "hoge"
  let! p2 = Dao.findPerson 2
  return p1 @ [p2] }

let config = 
  let dataConvRepo =
    let repo = DataConvRepo()
    repo.Add(Email.conv)
    repo.Add(Age.conv)
    repo.Add(Version.conv)
    repo
  let connectionString = "Data Source=.\SQLEXPRESS;Initial Catalog=tempdb;Integrated Security=True;" 
  { Dialect = MsSqlDialect(dataConvRepo)
    ConnectionProvider = fun () -> new SqlConnection(connectionString) :> DbConnection
    Logger = fun stmt -> printfn "LOG: %s" stmt.FormattedText }

let eval txBlock =
  match Tx.eval config txBlock with
  | Success ret -> printfn "success: %A\n" ret
  | Failure exn -> printfn "failure: %A\n" exn

[<EntryPoint>]
let main argv = 
  printfn "------------- workflow1 -------------"
  eval workflow1
  printfn "------------- workflow2 -------------"
  eval workflow2
  System.Console.ReadKey() |> fun _ -> 0
