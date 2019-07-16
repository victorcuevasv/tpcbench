
$env:Path += ";C:\Program Files\Inkscape"

$DATA_DIR="."

echo ''
echo "PROCESSING FILES IN DIRECTORY $DATA_DIR"
echo ''

#Load the data into the tables by psql instructions
$files = Get-ChildItem $DATA_DIR
    for ($i=0; $i -lt $files.Count; $i++) {
		$extn = [IO.Path]::GetExtension($files[$i])
		if ($extn -eq ".emf" )
		{
			#In PowerShell the extension is removed from the BaseName by default
			$baseName = $files[$i].BaseName 	
			$stmtToInvoke = 'inkscape -f=".\$baseName.emf" --export-pdf=".\$baseName.pdf"'
			$stmtToPrint = "inkscape -f=.\$baseName.emf --export-pdf=.\$baseName.pdf"			
			echo $stmtToPrint
			Invoke-Expression $stmtToInvoke
			echo "Press enter."
			$key = $Host.UI.RawUI.ReadKey()
		}
	}

echo ''
echo 'DONE' 
echo ''



