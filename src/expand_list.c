#include <Rinternals.h>

SEXP cartesian(SEXP lists) {
  size_t nlists = length(lists);
  size_t ncomb = 1;
  for(size_t i = 0; i < nlists; i++)
    ncomb *= length(VECTOR_ELT(lists, i));

  if(ncomb == 0) return allocVector(VECSXP, 0);

  SEXP oll = PROTECT(allocVector(VECSXP, ncomb)),
    names = getAttrib(lists, R_NamesSymbol);
  for(size_t i = 0; i < ncomb; i++) {
    SEXP x = PROTECT(allocVector(VECSXP, nlists));
    setAttrib(x, R_NamesSymbol, names);
    SET_VECTOR_ELT(oll, i, x);
  }
  UNPROTECT(ncomb);

  size_t runlen = 1;
  for(size_t i = 0; i < nlists; i++) {
    SEXP l = VECTOR_ELT(lists, i);
    size_t llen = length(l),
      nruns = ncomb / llen / runlen,
      stride = runlen*llen;
    for(size_t j = 0, jstart = 0; j < llen; j++, jstart+=runlen) {
      SEXP x = VECTOR_ELT(l, j);
      for(size_t run = 0, runstart = jstart; run < nruns; run++, runstart+=stride) {
        size_t runend = runstart + runlen;
        for(size_t k = runstart; k < runend; k++) {
          SET_VECTOR_ELT(VECTOR_ELT(oll, k), i, x);
        }
      }
    }
    runlen *= llen;
  }

  UNPROTECT(1);
  return oll;
}
