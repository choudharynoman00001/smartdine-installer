; SmartDine POS - Windows installer (NSIS)
; Portable Node runtime + app, per-user install (no admin required).
!include "MUI2.nsh"

Name "SmartDine POS"
OutFile "SmartDinePOS-Setup.exe"
InstallDir "$LOCALAPPDATA\SmartDine"
RequestExecutionLevel user

!define MUI_ABORTWARNING
!insertmacro MUI_PAGE_WELCOME
!insertmacro MUI_PAGE_DIRECTORY
!insertmacro MUI_PAGE_INSTFILES
!insertmacro MUI_PAGE_FINISH
!insertmacro MUI_UNPAGE_CONFIRM
!insertmacro MUI_UNPAGE_INSTFILES
!insertmacro MUI_LANGUAGE "English"

Section "Install"
  SetOutPath "$INSTDIR"
  File /r "stage\*.*"
  ; launcher: starts the server and opens the app
  FileOpen $0 "$INSTDIR\SmartDine.bat" w
  FileWrite $0 '@echo off$\r$\n'
  FileWrite $0 'cd /d "%~dp0app"$\r$\n'
  FileWrite $0 'if not defined SMARTDINE_NO_BROWSER start "" http://localhost:8080$\r$\n'
  FileWrite $0 '"%~dp0node\node.exe" server.mjs$\r$\n'
  FileClose $0
  CreateDirectory "$SMPROGRAMS\SmartDine"
  CreateShortcut "$SMPROGRAMS\SmartDine\SmartDine POS.lnk" "$INSTDIR\SmartDine.bat" "" "$INSTDIR\node\node.exe" 0
  CreateShortcut "$DESKTOP\SmartDine POS.lnk" "$INSTDIR\SmartDine.bat" "" "$INSTDIR\node\node.exe" 0
  WriteUninstaller "$INSTDIR\Uninstall.exe"
SectionEnd

Section "Uninstall"
  Delete "$SMPROGRAMS\SmartDine\SmartDine POS.lnk"
  RMDir "$SMPROGRAMS\SmartDine"
  Delete "$DESKTOP\SmartDine POS.lnk"
  RMDir /r "$INSTDIR"
SectionEnd
