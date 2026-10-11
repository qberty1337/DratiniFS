/* tmhooks.S - ARK Time Machine / DC compatibility (must be the FIRST object linked: text offset 0).
 *
 * ARK's TM loader (loader/dc/tmctrl/main.c) hooks fatms by overwriting the words at text+0x30fc,
 * +0x3ba4 and +0x44cc - Sony's `jal sceKernelExtendKernelStack` in IoOpen / IoDopen / IoDevctl -
 * with a jal to its own wrapper, which calls ExtendKernelStack(a0, a1, a2) itself and retries on
 * EMFILE after closing an emulated-flash file (reading the fpath pointer at arg+4).
 * Each trampoline puts the same jal at the same offset, so under TM the hook replaces a call
 * instead of corrupting unrelated code. Elsewhere it is a plain ExtendKernelStack call.
 * The padding below is zeros (= nops) and compresses away in the shipped PRX. */
	.set noreorder
	.set noat
	.text

	.macro TRAMP name, jalat
	.org \jalat - 8
	.globl \name
	.ent \name
\name:
	addiu	$sp, $sp, -16
	sw	$ra, 0($sp)
	jal	sceKernelExtendKernelStack	/* <- text+\jalat */
	nop
	lw	$ra, 0($sp)
	jr	$ra
	addiu	$sp, $sp, 16
	.end \name
	.endm

#ifdef PSPGO	/* fatmsef: the same call sites, 0x48 later (ARK hooks "sceFATFS_Driver" there on a Go) */
	TRAMP tm_ext_open,   0x3144
	TRAMP tm_ext_dopen,  0x3bec
	TRAMP tm_ext_devctl, 0x4514
#else
	TRAMP tm_ext_open,   0x30fc
	TRAMP tm_ext_dopen,  0x3ba4
	TRAMP tm_ext_devctl, 0x44cc
#endif
