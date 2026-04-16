const exactTranslationMap = {
  'Lagun grain cowhide leather watch straps Khaki': 'Da bò vân lagun màu kaki',
  'Sully grain goatskin leather watch straps Noir black': 'Da dê vân sully màu noir',
  'Epsom grain calfskin leather watch straps HAAS Cream': 'Da bê vân Epsom HAAS màu kem',
  'CLOE grain cowhide leather watch straps Black': 'Da bò vân CLOE màu đen',
  'Epsom grain calfskin leather watch straps Chocolate': 'Da bê vân Epsom màu Chocolate',
  'Epsom grain calfskin leather watch straps Taupe grey brown': 'Da bê vân Epsom màu ghi nâu',
  'Lagun grain cowhide leather wallet Khaki': 'Da bò vân lagun màu kaki',
  'Sully grain goatskin leather wallet Noir': 'Da dê vân sully màu noir',
  'Epsom grain calfskin leather wallet HAAS Cream': 'Da bê vân Epsom HAAS màu kem',
  'CLOE grain cowhide leather wallet Black': 'Da bò vân CLOE màu đen',
  'Epsom grain calfskin leather wallet Chocolate': 'Da bê vân Epsom màu Chocolate',
  'Epsom grain calfskin leather wallet Taupe grey brown': 'Da bê vân Epsom màu ghi nâu'
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