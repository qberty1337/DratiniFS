	.set noreorder

/* SysMemUserForUser setters for the compiled SDK version (uofw sysmem exports.exp). fatms reads
 * it to choose the name encoding: < 3.80 = Shift-JIS, >= 3.80 = UTF-8. Each accepts only its own
 * version (0x0307xxxx / 0x0606xxxx); anything else hangs the PSP, so main.c passes fixed values.
 *
 * pspsdk's libpspuser already has a SysMemUserForUser import entry, and its stubs link far from
 * these, which psp-fixup-imports can't merge ("stubs out of order"). So these two get their own
 * import entry for the same library (same flags as pspsdk's: 0x40000000), written out by hand
 * because IMPORT_START would redefine pspsdk's __stub_module_SysMemUserForUser. */

	.section .rodata.sceResident, "a"
	.word   0
sdkver_libname:
	.asciz  "SysMemUserForUser"
	.align  2

	.section .lib.stub, "a", @progbits
sdkver_stub_module:
	.word   sdkver_libname
	.word   0x40000000
	.word   0x5
	.word   __executable_start
	.word   __executable_start

	.macro SDKVER_FUNC nid, name
	.section .sceStub.text, "ax", @progbits
	.globl  \name
	.type   \name, @function
	.ent    \name, 0
\name:
	.word   sdkver_stub_module
	.word   \nid
	.end    \name
	.size   \name, .-\name
	.section .rodata.sceNid, "a"
	.word   \nid
	.endm

	SDKVER_FUNC 0x342061E5, sceKernelSetCompiledSdkVersion370
	SDKVER_FUNC 0x358CA1BB, sceKernelSetCompiledSdkVersion660
