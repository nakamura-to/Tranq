properties { 
    $baseDir  = resolve-path ..
    $buildDir = "$baseDir\build"
    $workDir = "$buildDir\work"
    $nugetDir = "$workDir\nuget.Tranq"
    $gtx_nugetDir = "$workDir\nuget.Tranq.GlobalTx"
    $nuspecFileName = "$nugetDir\Tranq.nuspec"
    $gtx_nuspecFileName = "$gtx_nugetDir\Tranq.GlobalTx.nuspec"
    $assemblyFileName = "$baseDir\src\Tranq\AssemblyInfo.fs"
    $gtx_assemblyFileName = "$baseDir\src\Tranq.GlobalTx\AssemblyInfo.fs"
    $assemblyVersionNumber = "0.0.0.0"
}

task default -depends ShowProperties

task ShowProperties {
    "`$baseDir = $baseDir"
    "`$buildDir = $buildDir"
    "`$assemblyFileName = $assemblyFileName"
    "`$gtx_assemblyFileName = $gtx_assemblyFileName"
    "`$workDir = $workDir"
    "`$assemblyVersionNumber = $assemblyVersionNumber"
}

task Clean -depends ShowProperties {
    Set-Location $baseDir
    if (Test-Path -path $workDir)
    {
        Write-Output -ForegroundColor Green "Deleting $workDir"    
        del $workDir -Recurse -Force
    }
    New-Item -Path $workDir -ItemType Directory
    New-Item -Path $nugetDir -ItemType Directory
    New-Item -Path $gtx_nugetDir -ItemType Directory
    New-Item -Path $nugetDir\lib -ItemType Directory
    New-Item -Path $gtx_nugetDir\lib -ItemType Directory
    Copy-Item -Path $buildDir\Tranq.nuspec -Destination $nuspecFileName
    Copy-Item -Path $buildDir\Tranq.GlobalTx.nuspec -Destination $gtx_nuspecFileName
}

function replaceVersion($assemblyFileName, $nuspecFileName) {
    $assemblyVersionPattern = 'AssemblyVersion\("[0-9]+(\.([0-9]+|\*)){1,3}"\)'
    $assemblyVersion = 'AssemblyVersion("' + $assemblyVersionNumber + '")';
    (Get-Content $assemblyFileName) -replace $assemblyVersionPattern, $assemblyVersion | Set-Content $assemblyFileName

    $nuspecVersionPattern = '<version>[0-9]+(\.([0-9]+|\*)){1,3}</version>'
    $nuspecVersion = "<version>$assemblyVersionNumber</version>";
    (Get-Content $nuspecFileName) -replace $nuspecVersionPattern, $nuspecVersion | Set-Content $nuspecFileName

    $nuspecDependencyVersionPattern = '<dependency id="Tranq" version="[0-9]+(\.([0-9]+|\*)){1,3}" />'
    $nuspecDependencyVersion = "<dependency id=`"Tranq`" version=`"$assemblyVersionNumber`" />";
    (Get-Content $nuspecFileName) -replace $nuspecDependencyVersionPattern, $nuspecDependencyVersion | Set-Content $nuspecFileName
}

task UpdateVersion -depends Clean {
    replaceVersion $assemblyFileName $nuspecFileName
    replaceVersion $gtx_assemblyFileName $gtx_nuspecFileName
}

task BuildAndTest -depends UpdateVersion {
    Write-Host -ForegroundColor Green "Testing"
    Write-Host
    exec { msbuild "/t:Clean;Rebuild" /p:Configuration=Release /p:OutputPath=$workDir\Tranq.Test "$baseDir\tests\Tranq.Test\Tranq.Test.fsproj" } "Error Build"
    exec { .\tools\NUnit-2.6.2\bin\nunit-console.exe "$workDir\Tranq.Test\Tranq.Test.dll" /framework=$framework /xml:$workDir\Tranq.Test\testResult.xml } "Error running $name tests" 
}

task NuGet -depends BuildAndTest {
    Write-Host -ForegroundColor Green "NuGet"
    Write-Host

    robocopy $workDir\Tranq.Test $nugetDir\lib\Net40 Tranq.dll Tranq.xml /MIR /NP
    exec { .\tools\NuGet\NuGet.exe pack $nuspecFileName }

    robocopy $workDir\Tranq.Test $gtx_nugetDir\lib\Net40 Tranq.GlobalTx.dll Tranq.GlobalTx.xml /MIR /NP
    exec { .\tools\NuGet\NuGet.exe pack $gtx_nuspecFileName }

    move -Path .\*.nupkg -Destination $workDir
}