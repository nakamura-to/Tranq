properties { 
    $baseDir  = resolve-path ..
    $buildDir = "$baseDir\build"
    $workDir = "$buildDir\work"
    $packageDir = "$workDir\package"
    $nugetDir = "$workDir\nuget"
    $nuspecFileName = "$nugetDir\Tranq.nuspec"
    $assemblyFileName = "$baseDir\src\Tranq\AssemblyInfo.fs"
    $assemblyVersionNumber = "0.0.0.0"
}

task default -depends ShowProperties

task ShowProperties {
    "`$baseDir = $baseDir"
    "`$buildDir = $buildDir"
    "`$assemblyFileName = $assemblyFileName"
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
    New-Item -Path $packageDir -ItemType Directory
    New-Item -Path $nugetDir -ItemType Directory
    New-Item -Path $nugetDir\lib -ItemType Directory
    Copy-Item -Path $buildDir\Tranq.nuspec -Destination $nuspecFileName
}

task UpdateVersion -depends Clean {
    $assemblyVersionPattern = 'AssemblyVersion\("[0-9]+(\.([0-9]+|\*)){1,3}"\)'
    $assemblyVersion = 'AssemblyVersion("' + $assemblyVersionNumber + '")';
    (Get-Content $assemblyFileName) -replace $assemblyVersionPattern, $assemblyVersion | Set-Content $assemblyFileName

    $nuspecVersionPattern = '<version>[0-9]+(\.([0-9]+|\*)){1,3}</version>'
    $nuspecVersion = "<version>$assemblyVersionNumber</version>";
    (Get-Content $nuspecFileName) -replace $nuspecVersionPattern, $nuspecVersion | Set-Content $nuspecFileName
}

task Build -depends UpdateVersion {
    Write-Host -ForegroundColor Green "Building"
    Write-Host
    exec { msbuild "/t:Clean;Rebuild" /p:Configuration=Release /p:OutputPath=$workDir\Tranq "$baseDir\src\Tranq\Tranq.fsproj" } "Error Build"
}

task Test -depends Build {
    Write-Host -ForegroundColor Green "Testing"
    Write-Host
    exec { msbuild "/t:Clean;Rebuild" /p:Configuration=Release /p:OutputPath=$workDir\Tranq.Test "$baseDir\tests\Tranq.Test\Tranq.Test.fsproj" } "Error Build"
    exec { .\tools\NUnit-2.6.2\bin\nunit-console.exe "$workDir\Tranq.Test\Tranq.Test.dll" /framework=$framework /xml:$workDir\Tranq.Test\testResult.xml } "Error running $name tests" 
}

task Package -depends Test {
    Write-Host -ForegroundColor Green "Packaging Tranq-$assemblyVersionNumber.zip"
    Write-Host
    robocopy $workDir\Tranq $packageDir Tranq.* /MIR /NP
    exec { .\tools\7za920\7za.exe a -tzip $workDir\Tranq-$assemblyVersionNumber.zip $packageDir\* } "Error zipping"
}

task NuGet -depends Package {
    Write-Host -ForegroundColor Green "NuGet"
    Write-Host
    Copy-Item -Path $packageDir -Destination $nugetDir\lib\Net40 -recurse
    exec { .\tools\NuGet\NuGet.exe pack $nuspecFileName }
    move -Path .\*.nupkg -Destination $workDir
}