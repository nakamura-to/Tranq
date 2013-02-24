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

module AssemblyInfo
open System.Reflection
open System.Runtime.CompilerServices;
[<assembly:AssemblyDescription("Tranq.dll")>]
[<assembly:AssemblyCompany("http://soma.codeplex.com/")>]
[<assembly:AssemblyTitle("Tranq.dll")>]
[<assembly:AssemblyCopyright("Copyright the Tranq Team.  All rights reserved.")>]
[<assembly:AssemblyProduct("Tranq")>]
[<assembly:AssemblyVersion("0.1.0.0")>]
#if DEBUG
[<assembly:InternalsVisibleTo("Tranq.Test")>]
#endif
do()
