#Requires -Modules dbatools
#Requires -Version 3.0


$inventoryfile = "$PSScriptRoot\jsonname.json"

#Reading the JSON for use in future variables. 
$appSQLBackup = Get-Content -Raw -Path $inventoryfile | ConvertFrom-Json

<#

Written By: Jamie Hutchings - IRM SQL DBA
Date: 1/19/2021
Purpose: I wrote this to restore backups from Prod to UAT. 
You will need to make sure to change the JSON values of the uploaded file. 
You need to specify the env that you want to refresh in order for this to work using the -bnymDB switch. 


.PARAMETER bnymDB
    This is the preprod env which will be refreshed. 

.NOTES 
This module is HEAVILY is dependent upon the dbatools module (dbatools.io), without this module installed, this function will not work in any way. 

e.g. 

Invoke-BNYMPreProdRefresh -bnymDB MDC will output the following before continuing:
'Which database do you want to refresh from pre-prod? Please enter mdc or pmj'


.EXAMPLE 
Invoke-BNYMPreProdRefresh -application DCI


#>


function Invoke-BNYMPreProdRefresh {
  [CmdletBinding()]
  param
  (
  
    [Parameter(Mandatory = $true,
      ValueFromPipeline = $true,
      ValueFromPipelineByPropertyName = $true)]
    [ValidateSet("changeme", "changeme", "changeme", "changeme")]
    [string]$application
  
  
  )

  Process {

    $dbApp = $appSQLBackup | Where-Object { $_.Application -eq "$application" }

    Switch ($application) {

      changeme {
        $PreProdListenerName = $dbApp.PreProdListenerName | Where-Object { $_.Application -eq "$application" }
      }
      changeme {
        $PreProdListenerName = $dbApp.PreProdListenerName | Where-Object { $_.Application -eq "$application" }
      }
      changeme {
        $PreProdListenerName = $dbApp.PreProdListenerName | Where-Object { $_.Application -eq "$application" }
      }
      changeme {
        $PreProdListenerName = $dbApp.PreProdListenerName | Where-Object { $_.Application -eq "$application" }
      }
    }

    #Finding current primary prod replica so  that the correct backup is copied 
    $ProdPrimaryReplica = Get-DbaAvailabilityGroup -SqlInstance $dbApp.ProdListenerName | Select-Object -ExpandProperty PrimaryReplicaServerName

    #Getting the current pre-prod primary replica so we know where to copy the backup to
    $PreProdPrimaryReplica = Get-DbaAvailabilityGroup -SqlInstance $dbApp.PreProdListenerName | Select-Object -ExpandProperty PrimaryReplicaServerName

    #Getting the current secondary replica for use later on to re-add db to AG. 
    $PreProdSecondaryReplica = Invoke-DbaQuery -SqlInstance $dbApp.PreProdListenerName -Query "
                                                          WITH AGStatus AS(
                                                          SELECT
                                                          name as AGname,
                                                          replica_server_name,
                                                          CASE WHEN  (primary_replica  = replica_server_name) THEN  1
                                                          ELSE  '' END AS IsPrimaryServer,
                                                          secondary_role_allow_connections_desc AS ReadableSecondary,
                                                          [availability_mode]  AS [Synchronous],
                                                          failover_mode_desc
                                                          FROM master.sys.availability_groups Groups
                                                          INNER JOIN master.sys.availability_replicas Replicas ON Groups.group_id = Replicas.group_id
                                                          INNER JOIN master.sys.dm_hadr_availability_group_states States ON Groups.group_id = States.group_id
                                                          )
                                                          
                                                          Select
                                                          replica_server_name
                                                          FROM AGStatus
                                                          WHERE
                                                          IsPrimaryServer = 0
                                                          AND Synchronous = 1
                                                          ORDER BY
                                                          AGname ASC,
                                                          IsPrimaryServer DESC" | Select-Object -ExpandProperty replica_server_name

    #Building the preprod destination string. This takes the results of the $PreProdPrimary query and combines it to the automated restore location from the JSON
    $PreProdDestination = '\\' + $PreProdPrimaryReplica + $dbApp.PreProdDestinationFolder

    #This code block is looking at the backup directory on the primary and finding the latest full backup and copying it to pre-prod

    $ProdRemoteBackupDir = "\\" + $ProdPrimaryReplica + $dbApp.ProdBackupDir
    $LatestBackup = Get-ChildItem -Path $ProdRemoteBackupDir | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    Write-Host "Copying $LatestBackup to $PreProdDestination"
    Start-BitsTransfer -DisplayName BackupCopy -Source $LatestBackup.FullName -Destination $PreProdDestination

    #This code block generates the restore file and outputs it to G:\AutomatedRestore folder
    $RestoreFile = Get-ChildItem -Path $PreProdDestination | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    Write-Host = $RestoreFile

      $result = Restore-DbaDatabase -SqlInstance $dbApp.PreProdListenerName -Path $RestoreFile.FullName -OutputScriptOnly -WithReplace 
      $result | Out-File -FilePath $PreProdDestination\Restore.sql -Force
    
    #Disables log backup job so we can use the same full backup on the secondary node later on
    Set-DbaAgentJob $dbApp.PreProdListenerName -Job $dbApp.DisableJobs -Disabled



    #This code block automatically takes the database offline and kicks off the restore on the primary
    Write-Host "Restoring prod to pre-prod"
    Remove-DbaAgDatabase -SqlInstance $dbApp.PreProdListenerName -Database $application 
    Invoke-DbaQuery -SqlInstance $dbApp.PreProdListenerName -Query "ALTER DATABASE $application SET OFFLINE WITH ROLLBACK IMMEDIATE" -QueryTimeout 0
    Invoke-DbaQuery -SqlInstance $dbApp.PreProdListenerName -File "$PreProdDestination\Restore.sql" -QueryTimeout 0


    #This code block adds the db back to the AG using automatic seeding. It first drops  the db on the secondary.

    Invoke-DbaQuery -SqlInstance $PreProdSecondaryReplica -Query "DROP DATABASE $application" -QueryTimeout 0
    Add-DbaAgDatabase -SqlInstance $dbApp.PreProdListenerName -AvailabilityGroup $dbApp.PreProdListenerName -Database $application -SeedingMode Automatic



    #This code block re-enables log backup
    Set-DbaAgentJob $dbApp.PreProdListenerName -Job $dbApp.DisableJobs -Enabled
    Remove-Item $PreProdDestination\*.*

  }
}




  
