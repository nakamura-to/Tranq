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
open System.Globalization
open System.Reflection
open System.Resources

type Message = 
  { Id : string;
    Text : string }

[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Message =
  let format {Id = id; Text = text} = 
    "[" + id + "] " + text

  let appendText {Id = id; Text = text} tailText =
    { Id = id; Text = text + " " + tailText }

[<RequireQualifiedAccess>]
module internal SR =

  let private resources = 
    new ResourceManager("Resource", Assembly.GetExecutingAssembly())

  let private message id (args:obj array) =
    let resource = resources.GetString(id, CultureInfo.CurrentUICulture)
    let text = 
      if Array.isEmpty args then 
        resource 
      else
        String.Format(resource, args)
    { Id = id; Text = text }

  let private message0 (id, args:unit) = 
    message id Array.empty
  
  let private message1 (id, args:obj) = 
    message id [| args |]
  
  let private message2 (id, (arg1:obj, arg2:obj)) = 
    message id [| arg1; arg2 |]
   
  let private message3 (id, (arg1:obj, arg2:obj, arg3:obj)) = 
    message id [| arg1; arg2; arg3 |]

  let private message4 (id, (arg1:obj, arg2:obj, arg3:obj, arg4:obj)) = 
    message id [| arg1; arg2; arg3; arg4 |]

  let TRANQ0001 args = message0 ("TRANQ0001", args)
  let TRANQ0002 args = message1 ("TRANQ0002", args)

  (* expr *)
  let TRANQ1000 args = message2 ("TRANQ1000", args)
  let TRANQ1001 args = message3 ("TRANQ1001", args)
  let TRANQ1002 args = message2 ("TRANQ1002", args)
  let TRANQ1003 args = message2 ("TRANQ1003", args)
  let TRANQ1004 args = message2 ("TRANQ1004", args)
  let TRANQ1005 args = message1 ("TRANQ1005", args)
  let TRANQ1006 args = message1 ("TRANQ1006", args)
  let TRANQ1007 args = message2 ("TRANQ1007", args)
  let TRANQ1009 args = message2 ("TRANQ1009", args)
  let TRANQ1011 args = message2 ("TRANQ1011", args)
  let TRANQ1012 args = message0 ("TRANQ1012", args)
  let TRANQ1013 args = message0 ("TRANQ1013", args)
  let TRANQ1014 args = message1 ("TRANQ1014", args)
  let TRANQ1015 args = message0 ("TRANQ1015", args)
  let TRANQ1016 args = message0 ("TRANQ1016", args)
  let TRANQ1017 args = message0 ("TRANQ1017", args)
  let TRANQ1018 args = message1 ("TRANQ1018", args)
  let TRANQ1019 args = message1 ("TRANQ1019", args)
  let TRANQ1020 args = message3 ("TRANQ1020", args)
  let TRANQ1021 args = message0 ("TRANQ1021", args)
  let TRANQ1022 args = message0 ("TRANQ1022", args)
  let TRANQ1023 args = message1 ("TRANQ1023", args)
  let TRANQ1024 args = message2 ("TRANQ1024", args)
  let TRANQ1025 args = message1 ("TRANQ1025", args)
  let TRANQ1026 args = message2 ("TRANQ1026", args)
  let TRANQ1027 args = message0 ("TRANQ1027", args)

  (* sql *)
  let TRANQ2000 args = message1 ("TRANQ2000", args)
  let TRANQ2001 args = message0 ("TRANQ2001", args)
  let TRANQ2002 args = message0 ("TRANQ2002", args)
  let TRANQ2003 args = message0 ("TRANQ2003", args)
  let TRANQ2004 args = message1 ("TRANQ2004", args)
  let TRANQ2005 args = message0 ("TRANQ2005", args)
  let TRANQ2006 args = message3 ("TRANQ2006", args)
  let TRANQ2007 args = message0 ("TRANQ2007", args)
  let TRANQ2008 args = message1 ("TRANQ2008", args)
  let TRANQ2009 args = message1 ("TRANQ2009", args)
  let TRANQ2010 args = message0 ("TRANQ2010", args)
  let TRANQ2011 args = message0 ("TRANQ2011", args)
  let TRANQ2012 args = message0 ("TRANQ2012", args)
  let TRANQ2013 args = message0 ("TRANQ2013", args)
  let TRANQ2014 args = message2 ("TRANQ2014", args)
  let TRANQ2015 args = message1 ("TRANQ2015", args)
  let TRANQ2016 args = message0 ("TRANQ2016", args)
  let TRANQ2017 args = message0 ("TRANQ2017", args)
  let TRANQ2018 args = message1 ("TRANQ2018", args)
  let TRANQ2019 args = message1 ("TRANQ2019", args)

  (* meta *)
  let TRANQ3000 args = message3 ("TRANQ3000", args)
  let TRANQ3002 args = message1 ("TRANQ3002", args)
  let TRANQ3003 args = message1 ("TRANQ3003", args)
  let TRANQ3004 args = message1 ("TRANQ3004", args)
  let TRANQ3005 args = message2 ("TRANQ3005", args)
  let TRANQ3006 args = message2 ("TRANQ3006", args)
  let TRANQ3007 args = message1 ("TRANQ3007", args)
  let TRANQ3008 args = message1 ("TRANQ3008", args)
  let TRANQ3009 args = message2 ("TRANQ3009", args)

  (* db *)
  let TRANQ4002 args = message0 ("TRANQ4002", args)
  let TRANQ4003 args = message2 ("TRANQ4003", args)
  let TRANQ4004 args = message0 ("TRANQ4004", args)
  let TRANQ4005 args = message1 ("TRANQ4005", args)
  let TRANQ4010 args = message0 ("TRANQ4010", args)
  let TRANQ4011 args = message2 ("TRANQ4011", args)
  let TRANQ4012 args = message3 ("TRANQ4012", args)
  let TRANQ4013 args = message2 ("TRANQ4013", args)
  let TRANQ4014 args = message3 ("TRANQ4014", args)
  let TRANQ4015 args = message2 ("TRANQ4015", args)
  let TRANQ4016 args = message2 ("TRANQ4016", args)
  let TRANQ4017 args = message4 ("TRANQ4017", args)
  let TRANQ4018 args = message4 ("TRANQ4018", args)
  let TRANQ4019 args = message2 ("TRANQ4019", args)
  let TRANQ4022 args = message3 ("TRANQ4022", args)
  let TRANQ4023 args = message4 ("TRANQ4023", args)
  let TRANQ4024 args = message0 ("TRANQ4024", args)
  let TRANQ4025 args = message0 ("TRANQ4025", args)
  let TRANQ4027 args = message3 ("TRANQ4027", args)
  let TRANQ4028 args = message0 ("TRANQ4028", args)
  let TRANQ4029 args = message1 ("TRANQ4029", args)
  let TRANQ4030 args = message1 ("TRANQ4030", args)

  (* other *)
  let TRANQ5001 args = message0 ("TRANQ5001", args)
  let TRANQ5002 args = message0 ("TRANQ5002", args)