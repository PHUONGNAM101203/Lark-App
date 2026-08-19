const exactTranslationMap = {
  'Sully goatskin leather watch straps Violine': 'Da dê vân sully màu Violine',
  'Epsom calfskin leather watch straps Rouge': 'Da bê vân Epsom HAAS Màu Đỏ (Rouge)',
  'Sully goatskin leather watch straps Rose': 'Da dê vân sully màu Rose',
  'Epsom calfskin leather watch straps Navy Blue': 'Da bê vân Epsom HAAS Màu Xanh Navy (Navy Blue)',
  'Sully goatskin leather watch straps Light Cream': 'Da dê vân Sully Màu Light Cream',
  'Epsom calfskin leather watch straps Gold': 'Da bê vân Epsom Màu Nâu vàng (Gold)',
  'Lagun cowhide leather watch straps Ficelle': 'Da bò vân Lagun Màu Trắng Đục (Ficelle)',
  'Sully goatskin leather watch straps Noisette': 'Da dê vân Sully Màu Noisette',
  'Sully grain goatskin leather wallet Violine': 'Da dê vân sully màu Violine',
  'Sully goatskin leather wallet Violine': 'Da dê vân sully màu Violine',
  'Epsom calfskin leather wallet Rouge': 'Da bê vân Epsom HAAS Màu Đỏ (Rouge)',
  'Sully grain goatskin leather wallet Rose': 'Da dê vân sully màu Rose',
  'Sully goatskin leather wallet Rose': 'Da dê vân sully màu Rose',
  'Epsom calfskin leather wallet Navy Blue': 'Da bê vân Epsom HAAS Màu Xanh Navy (Navy Blue)',
  'Sully goatskin leather wallet Light Cream': 'Da dê vân Sully Màu Light Cream',
  'Epsom calfskin leather wallet Gold': 'Da bê vân Epsom Màu Nâu vàng (Gold)',
  'Lagun cowhide leather wallet Ficelle': 'Da bò vân Lagun Màu Trắng Đục (Ficelle)',
  'Sully goatskin leather wallet Noisette': 'Da dê vân Sully Màu Noisette',
  'Epsom calfskin leather watch straps HAAS Cream': 'Da bê vân Epsom HAAS Màu Kem (Cream)',
  'Epsom calfskin leather watch straps Chocolate': 'Da bê vân Epsom Màu Chocolate',
  'Sully goatskin leather watch straps Acier': 'Da dê vân Sully Màu Acier',
  'Patnat goatskin leather watch straps Patchouli': 'Da dê vân Patnat màu Patechouli',
  'Lagun cowhide leather watch straps Orange': 'Da bò vân Lagun Màu Cam (Orange)',
  'Epsom calfskin leather watch straps Noir black': 'Da bê vân Epsom Màu Đen (Noir)',
  'Togo Odessa Weinheimer calfskin leather watch straps black': 'Da bê Odessa vân Togo weinheimer màu đen black',
  'Sully goatskin leather watch straps Bleu Vista': 'Da dê vân Sully Màu Bleu Vista',
  'Epsom calfskin leather wallet Etoupe': 'Da bê vân Epsom Màu Ghi Nâu (Etoupe)',
  'Sully goatskin leather wallet Myrrhe': 'Da dê vân Sully Màu Myrrhe',
  'Sully grain goatskin leather wallet Myrrhe': 'Da dê vân Sully Màu Myrrhe',
  'Sully grain goatskin leather wallet Colibri': 'Da dê vân Sully Màu Colibri',
  'Sully goatskin leather wallet Colibri': 'Da dê vân Sully Màu Colibri',
  'Epsom calfskin leather wallet HAAS Cream': 'Da bê vân Epsom HAAS Màu Kem (Cream)',
  'Epsom calfskin leather wallet Chocolate': 'Da bê vân Epsom Màu Chocolate',
  'Sully goatskin leather wallet Oursin': 'Da dê vân Sully Màu Oursin',
  'Sully grain goatskin leather wallet Oursin': 'Da dê vân Sully Màu Oursin'
};

function normalizeText(text) {
  return text ? text.trim().replace(/\s+/g, ' ') : '';
}

function getProductUnit(rawDesc) {
  const text = normalizeText(rawDesc).toLowerCase();
  if (text.includes('piece')) return 'Piece';
  return 'Pair'; // mặc định là Pair
}

function cleanProductKey(rawDesc) {
  let cleaned = normalizeText(rawDesc);
  cleaned = cleaned.replace(/^(a|\d+(\.\d+)?)\s+(pair|piece)s?\s+of\s+/i, '');
  return cleaned;
}

function translateProductName(rawDesc) {
  if (!rawDesc) return '';
  const exactKey = Object.keys(exactTranslationMap).find(key => normalizeText(key).toLowerCase() === normalizeText(rawDesc).toLowerCase());
  return exactKey ? exactTranslationMap[exactKey] : '';
}

module.exports = { translateProductName, cleanProductKey, getProductUnit };
