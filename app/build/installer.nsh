!include "MUI2.nsh"
!include "nsDialogs.nsh"
!include "LogicLib.nsh"

Var DataDir

!macro preInit
  StrCpy $DataDir "$DOCUMENTS"
!macroend

!macro customInit
  nsDialogs::SelectFolderDialog "Select where CourtVision should store logs and exports" "$DataDir"
  Pop $0
  ${If} $0 != ""
    StrCpy $DataDir $0
  ${EndIf}
!macroend

!macro customInstall
  CreateDirectory "$DataDir\CourtVision"
  CreateDirectory "$DataDir\CourtVision\Logs"
  CreateDirectory "$DataDir\CourtVision\Exports"
  CreateDirectory "$APPDATA\CourtVision"
  FileOpen $0 "$APPDATA\CourtVision\installer-storage-path.txt" w
  FileWrite $0 "$DataDir"
  FileClose $0
!macroend

