const exactTranslationMap = {
  'A pair of Lagun grain cowhide leather watch straps Khaki': 'Da bò vân lagun màu kaki',
  'A pair of Sully grain goatskin leather watch straps Noir black': 'Da dê vân sully màu noir',
  'A pair of Epsom grain calfskin leather watch straps HAAS Cream': 'Da bê vân Epsom HAAS màu kem',
  'A pair of CLOE grain cowhide leather watch straps Black': 'Da bò vân CLOE màu đen',
  'A pair of Epsom grain calfskin leather watch straps Chocolate': 'Da bê vân Epsom màu Chocolate',
  'A pair of Epsom grain calfskin leather watch straps Taupe grey brown': 'Da bê vân Epsom màu ghi nâu',
  'A piece of Lagun grain cowhide leather wallet Khaki': 'Da bò vân lagun màu kaki',
  'A piece of Sully grain goatskin leather wallet Noir': 'Da dê vân sully màu noir',
  'A piece of Epsom grain calfskin leather wallet HAAS Cream': 'Da bê vân Epsom HAAS màu kem',
  'A piece of CLOE grain cowhide leather wallet Black': 'Da bò vân CLOE màu đen',
  'A piece of Epsom grain calfskin leather wallet Chocolate': 'Da bê vân Epsom màu Chocolate',
  'A piece of Epsom grain calfskin leather wallet Taupe grey brown': 'Da bê vân Epsom màu ghi nâu'
};

const colorLookup = {
  noir: 'noir',
  black: 'đen',
  khaki: 'kaki',
  cream: 'kem',
  chocolate: 'Chocolate',
  'taupe grey brown': 'ghi nâu',
  'grey brown': 'ghi nâu',
  'taupe grey': 'ghi',
  grey: 'ghi',
  brown: 'nâu'
};

function normalizeText(text) {
  return text.trim().replace(/\s+/g, ' ');
}

function translateColorSegment(segment) {
  if (!segment) return '';
  const normalized = normalizeText(segment).toLowerCase();
  if (colorLookup[normalized]) return colorLookup[normalized];

  const words = normalized.split(' ');
  // preserve brand tokens and translate only the color part
  const brandTokens = [];
  const colorTokens = [];
  for (const word of words) {
    if (word === word.toUpperCase() && word.length > 1 && !['A', 'AN', 'OF', 'THE'].includes(word)) {
      brandTokens.push(word);
    } else {
      colorTokens.push(word);
    }
  }

  const colorPhrase = colorTokens.join(' ').trim();
  const resolvedColor = colorLookup[colorPhrase] || colorPhrase;
  return brandTokens.length > 0 ? `${brandTokens.join(' ')} ${resolvedColor}`.trim() : resolvedColor;
}

function translateProductName(rawDesc) {
  if (!rawDesc) return '';
  const original = normalizeText(rawDesc);
  if (exactTranslationMap[original]) return exactTranslationMap[original];

  const content = original.replace(/^(a pair of|a piece of)\s+/i, '').trim();
  const typeMatch = content.match(/^(.*?)\s+grain\s+(cowhide|goatskin|calfskin)\s+leather\s*(.*)$/i);
  if (!typeMatch) {
    return original;
  }

  const brand = typeMatch[1].trim();
  const leatherType = typeMatch[2].toLowerCase();
  const remainder = typeMatch[3].trim();
  const base = leatherType === 'cowhide' ? 'Da bò' : leatherType === 'goatskin' ? 'Da dê' : leatherType === 'calfskin' ? 'Da bê' : 'Da';

  let finalText = `${base} vân ${brand}`;

  let colorPart = remainder
    .replace(/^(watch\s+straps?|wallet|strap|band|case|pouch)\s*/i, '')
    .trim();

  if (colorPart) {
    colorPart = translateColorSegment(colorPart);
    if (colorPart) {
      finalText += ` màu ${colorPart}`;
    }
  }

  return finalText;
}

module.exports = { translateProductName };
