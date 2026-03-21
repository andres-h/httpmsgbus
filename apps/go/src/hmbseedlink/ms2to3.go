/***************************************************************************
 *   Copyright (C) by GFZ Potsdam                                          *
 *                                                                         *
 *   Author:  Andres Heinloo                                               *
 *   Email:   andres@gfz-potsdam.de                                        *
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2, or (at your option)   *
 *   any later version. For more information, see http://www.gnu.org/      *
 ***************************************************************************/

package main

/*
#cgo LDFLAGS: -L${SRCDIR}/libmseed -lmseed
#cgo CFLAGS: -I${SRCDIR}/libmseed

#include <libmseed.h>

static int ms2to3(const char *inptr, int inlen, char *outptr, int outlen) {
	MS3Record *msr = NULL;
	int retval;

	if(msr3_parse(inptr, inlen, &msr, 0, 0) != MS_NOERROR)
		return -1;

	retval = msr3_repack_mseed3(msr, outptr, outlen, 0);
	msr3_free(&msr);
	return retval;
}

*/
import "C"
import "unsafe"

func ms2to3(in []byte, out []byte) int {
	inptr := (*C.char)(unsafe.Pointer(&in[0]))
	inlen := C.int(len(in))
	outptr := (*C.char)(unsafe.Pointer(&out[0]))
	outlen := C.int(len(out))
	return int(C.ms2to3(inptr, inlen, outptr, outlen))
}
