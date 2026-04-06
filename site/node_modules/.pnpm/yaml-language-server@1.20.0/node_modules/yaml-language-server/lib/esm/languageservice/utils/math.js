export function floatSafeRemainder(val, step) {
    const valDecCount = (val.toString().split('.')[1] || '').length;
    const stepDecCount = (step.toString().split('.')[1] || '').length;
    const decCount = Math.max(valDecCount, stepDecCount);
    const valInt = parseInt(val.toFixed(decCount).replace('.', ''));
    const stepInt = parseInt(step.toFixed(decCount).replace('.', ''));
    return (valInt % stepInt) / Math.pow(10, decCount);
}
//# sourceMappingURL=math.js.map