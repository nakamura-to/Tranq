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

type Person = { 
  [<Id>]
  Id: int
  Email: Email.t
  Age: Age.t
  [<Version>]
  Version: Version.t }

[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Person =
  let incrAge person = { person with Age = Age.incr person.Age }

module PersonDao =
  type t = Person
  let all () = Db.query<t> "select * from Person" []
  let byEmail (email: Email.t) = Db.query<t> "select * from Person where Email = @Email" ["@Email" <-- email]
  let byId (id: int) = Db.find<t> [id]
  let insert person = Db.insert<t> person
  let update person = Db.update<t> person
  let delete person = Db.delete<t> person

// init databases
let init = 
  Db.run "
  if exists (select * from dbo.sysobjects where id = object_id(N'Person')) drop table Person;
  create table Person (Id int primary key, Email varchar(50), Age int, Version int);
  insert Person (Id, Email, Age, Version) values (1, 'hoge@example.com', 10, 0);
  insert Person (Id, Email, Age, Version) values (2, 'foo_example.com', null, 0);
  insert Person (Id, Email, Age, Version) values (3, 'bar@example.com', 30, 0);
  " []

/// query, increment age and then update
let workflow1 = txRequired {
  do! init
  let! persons = PersonDao.all()
  return! persons 
    |> List.map Person.incrAge 
    |> Tx.mapM PersonDao.update }

// query by name and query by identifier
let workflow2 = txRequired.With(TxIsolationLevel.ReadUncommitted) {
  do! init
  let email = Email.make "hoge@example.com"
  let! p1 = PersonDao.byEmail email
  let! p2 = PersonDao.byId 2
  return p1 @ [p2] }

// insert and delete
let workflow3 = txRequired {
  do! init
  let! person = PersonDao.insert {
    Id = 99
    Email = Email.make "fuga@example.com"
    Age = Age.make (Some 20)
    Version = Version.make 0 }
  do! PersonDao.delete person }

open Tranq.GlobalTx

/// query, increment age and then update
let gtx_workflow1 = txRequired {
  do! init
  let! persons = PersonDao.all()
  return! persons 
    |> List.map Person.incrAge 
    |> Tx.mapM PersonDao.update }

// query by name and query by identifier
let gtx_workflow2 = txRequired.With(TxIsolationLevel.ReadUncommitted) {
  do! init
  let email = Email.make "hoge@example.com"
  let! p1 = PersonDao.byEmail email
  let! p2 = PersonDao.byId 2
  return p1 @ [p2] }

// insert and delete
let gtx_workflow3 = txRequired {
  do! init
  let! person = PersonDao.insert {
    Id = 99
    Email = Email.make "fuga@example.com"
    Age = Age.make (Some 20)
    Version = Version.make 0 }
  do! PersonDao.delete person }

let config = 
  let registry = DataConvRegistry()
  registry.Add(Email.conv)
  registry.Add(Age.conv)
  registry.Add(Version.conv)
  let connectionString = "Data Source=.\SQLEXPRESS;Initial Catalog=tempdb;Integrated Security=True;" 
  let log = printfn "LOG: %s"
  let listener = function
    | TxBegun(txInfo, _, _)-> log (sprintf "txInfo=%A, tx begin" txInfo)
    | TxCommitted(txInfo, _, _)-> log (sprintf "txInfo=%A, tx commit" txInfo)
    | TxRolledback(txInfo, _, _)-> log (sprintf "txInfo=%A, tx rollback" txInfo)
    | SqlIssuing(txInfo, stmt) -> log (sprintf "txInfo=%A, sql=[%s]" txInfo stmt.FormattedText)
  { Dialect = MsSqlDialect(registry)
    ConnectionProvider = fun () -> new SqlConnection(connectionString) :> DbConnection
    Listener = listener }

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
  printfn "------------- workflow3 -------------"
  eval workflow3

  printfn "------------- gtx_workflow1 -------------"
  eval gtx_workflow1
  printfn "------------- gtx_workflow2 -------------"
  eval gtx_workflow2
  printfn "------------- gtx_workflow3 -------------"
  eval gtx_workflow3

  System.Console.ReadKey() |> fun _ -> 0
