# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['Production_Weekly_Extractor.py'],
    pathex=[],
    binaries=[],
    datas=[('PW_Extractor_Long-01-01.png', '.'), ('.venv\\Lib\\site-packages\\geonamescache\\data', 'geonamescache\\data')],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='Production_Weekly_Extractor',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=['PW_Extractor-01-01-01.ico'],
)
