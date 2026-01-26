!include "MUI2.nsh"
!include "nsDialogs.nsh"
!include "LogicLib.nsh"

Var DataDir
Var DataDirPage
Var DataDirText
Var DataDirBrowse

!define MUI_PRODUCT "CourtVision"
!define MUI_BRANDINGTEXT "NBA Player Performance Analytics with AI Predictions"

!macro preInit
  StrCpy $DataDir "$DOCUMENTS\CourtVision"
!macroend

Function DataDirPageCreate
  !insertmacro MUI_HEADER_TEXT "Choose Data Location" "Select where CourtVision should store logs and exports"
  
  nsDialogs::Create 1018
  Pop $DataDirPage
  
  ${If} $DataDirPage == error
    Abort
  ${EndIf}
  
  ${NSD_CreateLabel} 0 10u 100% 20u "Data will be stored in the following location:"
  Pop $R0
  
  ${NSD_CreateText} 0 35u 240u 12u "$DataDir"
  Pop $DataDirText
  
  ${NSD_CreateButton} 245u 34u 50u 14u "Browse..."
  Pop $DataDirBrowse
  ${NSD_OnClick} $DataDirBrowse DataDirBrowseClick
  
  ${NSD_CreateLabel} 0 55u 100% 30u "This folder will contain your exported images and error logs.$\r$\nYou can change this location later in Settings."
  Pop $R1
  
  nsDialogs::Show
FunctionEnd

Function DataDirBrowseClick
  ${NSD_GetText} $DataDirText $0
  nsDialogs::SelectFolderDialog "Select where CourtVision should store logs and exports" "$0"
  Pop $1
  ${If} $1 != ""
    StrCpy $DataDir "$1"
    ${NSD_SetText} $DataDirText "$DataDir"
  ${EndIf}
FunctionEnd

Function DataDirPageLeave
  ${NSD_GetText} $DataDirText $DataDir
  ${If} $DataDir == ""
    MessageBox MB_ICONEXCLAMATION "Please select a location for data storage."
    Abort
  ${EndIf}
FunctionEnd

Page custom DataDirPageCreate DataDirPageLeave

!macro customInstall
  CreateDirectory "$DataDir"
  CreateDirectory "$DataDir\Logs"
  CreateDirectory "$DataDir\Exports"
  CreateDirectory "$APPDATA\CourtVision"
  FileOpen $0 "$APPDATA\CourtVision\installer-storage-path.txt" w
  FileWrite $0 "$DataDir"
  FileClose $0
!macroend

